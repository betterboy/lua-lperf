
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdarg.h>
#include <signal.h>

#include <string>
#include <list>
#include <unordered_map>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <functional>
#include <vector>

extern "C"
{
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
// #include <lstate.h>
}

static void signal_profiler(int sig, siginfo_t *sinfo, void *ucontext);


struct CoroutineNode
{
    CoroutineNode *prev;
    CoroutineNode *next;
    lua_State *L;
};

struct Mapping;
struct ProfilerContext
{
    lua_State *L;
    CoroutineNode *head;
    CoroutineNode *tail;
    CoroutineNode *freelist;
    const char *data_file;
    Mapping *prof_data;

    int debug_mode;
    int log_fd;
};

ProfilerContext *profiler_ctx = NULL;

#define LOG_MAXLEN (4096)
#define FLOG_DEBUG(...) f_log("DEBUG", __FILE__, __LINE__, __VA_ARGS__)
#define FLOG_INFO(...) f_log("INFO", __FILE__, __LINE__, __VA_ARGS__)
#define FLOG_ERR(...) f_log("ERR", __FILE__, __LINE__, __VA_ARGS__)

static void f_log(const char *level, const char *file, int line, const char *fmt, ...)
{
    if (!profiler_ctx->debug_mode) return;

    char log_msg[LOG_MAXLEN];
    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm *tm_now = localtime(&tv.tv_sec);
    int header_len = sprintf(log_msg, "[%04d-%02d-%02d %02d:%02d:%02d][%s](%s:%d)", (tm_now->tm_year + 1900), tm_now->tm_mon + 1, tm_now->tm_mday, tm_now->tm_hour, tm_now->tm_min, tm_now->tm_sec, level, file, line);

    va_list ap;
    va_start(ap, fmt);
    int len = vsnprintf(log_msg + header_len, sizeof(log_msg), fmt, ap);
    va_end(ap);
    write(profiler_ctx->log_fd,  log_msg, header_len + len); 
    write(profiler_ctx->log_fd,  "\n", 1); 
}

/*-----copy from lua lauxlib.c---------*/
/*
** search for 'objidx' in table at index -1.
** return 1 + string at top if find a good name.
*/
static int findfield(lua_State *L, int objidx, int level) {
    if (level == 0 || !lua_istable(L, -1))
        return 0;  /* not found */
    lua_pushnil(L);  /* start 'next' loop */
    while (lua_next(L, -2)) {  /* for each pair in table */
        if (lua_type(L, -2) == LUA_TSTRING) {  /* ignore non-string keys */
            if (lua_rawequal(L, objidx, -1)) {  /* found object? */
                lua_pop(L, 1);  /* remove value (but keep name) */
                return 1;
            } else if (findfield(L, objidx, level - 1)) {  /* try recursively */
                lua_remove(L, -2);  /* remove table (but keep name) */
                lua_pushliteral(L, ".");
                lua_insert(L, -2);  /* place '.' between the two names */
                lua_concat(L, 3);
                return 1;
            }
        }
        lua_pop(L, 1);  /* remove value */
    }
    return 0;  /* not found */
}

/*
** Search for a name for a function in all loaded modules
*/
static int pushglobalfuncname(lua_State *L, lua_Debug *ar) {
    int top = lua_gettop(L);
    lua_getinfo(L, "f", ar);  /* push function */
    lua_getfield(L, LUA_REGISTRYINDEX, LUA_LOADED_TABLE);
    if (findfield(L, top + 1, 2)) {
        const char *name = lua_tostring(L, -1);
        if (strncmp(name, "_G.", 3) == 0) {  /* name start with '_G.'? */
            lua_pushstring(L, name + 3);  /* push name without prefix */
            lua_remove(L, -2);  /* remove original name */
        }
        lua_copy(L, -1, top + 1);  /* move name to proper place */
        lua_pop(L, 2);  /* remove pushed values */
        return 1;
    } else {
        lua_settop(L, top);  /* remove function and global table */
        return 0;
    }
}


static void pushfuncname(lua_State *L, lua_Debug *ar) {
    if (pushglobalfuncname(L, ar)) {  /* try first a global name */
        lua_pushfstring(L, "function '%s'", lua_tostring(L, -1));
        lua_remove(L, -2);  /* remove name */
    } else if (*ar->namewhat != '\0')  /* is there a name from code? */
        lua_pushfstring(L, "%s '%s'", ar->namewhat, ar->name);  /* use it */
    else if (*ar->what == 'm')  /* main? */
        lua_pushliteral(L, "main chunk");
    else if (*ar->what != 'C')  /* for Lua functions, use <file:line> */
        lua_pushfstring(L, "function %s:%d>", ar->short_src, ar->linedefined);
    else  /* nothing left... */
        lua_pushliteral(L, "?");
}


static int lastlevel(lua_State *L) {
    lua_Debug ar;
    int li = 1, le = 1;
    /* find an upper bound */
    while (lua_getstack(L, le, &ar)) {
        li = le;
        le *= 2;
    }
    /* do a binary search */
    while (li < le) {
        int m = (li + le) / 2;
        if (lua_getstack(L, m, &ar)) li = m + 1;
        else le = m;
    }
    return le - 1;
}

/*-----copy from lua lauxlib.c end---------*/
std::unordered_map<int, std::string> gId2Name;
std::unordered_map<std::string, int> gName2Id;

static void init_nameid()
{
    gName2Id["?"] = 0;
    gId2Name[0] = "?";
    gName2Id["function 'xpcall'"] = 1;
    gId2Name[1] = "function 'xpcall'";
	gName2Id["function 'pcall'"] = 2;
	gId2Name[2] = "function 'pcall'";
}

static int get_nameid(const char *name)
{
    int id = -1;
    auto iter = gName2Id.find(name);
    if (iter == gName2Id.end()) {
        id = gName2Id.size();
        gName2Id[name] = id;
        gId2Name[id] = name;
    } else {
        id = iter->second;
    }

    if (id < 3) {
        id = -1;
    }

    return id;
}

static const std::string get_idname(int id)
{
    auto iter = gId2Name.find(id);
    return iter->second;
}

static const int MAP_HASH_TABLE_SIZE = 1024;
static const int MAX_STACK_SIZE = 64;

struct CallStack
{
    CallStack *next;
    int count;
    int depth;
    int stack[MAX_STACK_SIZE];
};

struct Mapping
{
    CallStack **table;
    unsigned short table_size;
    int count;
};

static int node_hash(CallStack *cs)
{
    int hash = 0;
    for (int i = 0; i < cs->depth; i++) {
        int id = cs->stack[i];
        hash = (hash << 8) | (hash >> (8 * (sizeof(hash) - 1)));
        hash += (id * 31) + (id * 7) + (id * 3);
    }

    return hash;
}

static Mapping *allocate_mapping(int n)
{
    if (n > MAP_HASH_TABLE_SIZE) {
        n |= n >> 1;
		n |= n >> 2;
		n |= n >> 4;
		if (n &0xff00)
		{
			n |= n >> 8;
		}
    } else {
        n = MAP_HASH_TABLE_SIZE - 1;
    }

    Mapping *newmap = new Mapping();
    newmap->table_size = n++;
    newmap->table = new CallStack*[n];
    newmap->count = 0;
    memset(newmap->table, 0, n);

    return newmap;
}

static void mapping_insert(Mapping *map, CallStack *cs)
{
    int hash = node_hash(cs);
    unsigned short i = hash & map->table_size;
    CallStack *n = NULL;
    CallStack **bucket = map->table + i;
    int find = 0;
    if ((n = *bucket)) {
        do {
            if (n->depth == cs->depth && memcmp(n->stack, cs->stack, sizeof(int) * cs->depth) == 0) {
                find = 1;
                break;
            }
        } while ((n = n->next));
    }

    if (find) {
        n->count++;
    } else {
        cs->next = *bucket;
        *bucket = cs;
    }
    map->count++;
    FLOG_DEBUG("add new call stack. count=%d,hash=%d,i=%d,find=%d", map->count, hash, i, find);
}

static int mapping_save(Mapping *map)
{
    std::vector<CallStack *> arr;
    int j = map->table_size;
    CallStack *elt, **table = map->table;
    do {
        for (elt = table[j]; elt; elt = elt->next) {
            arr.push_back(elt);
        }
    } while (j--);

    std::sort(arr.begin(), arr.end(), [](CallStack *lhs, CallStack *rhs) {
        return lhs->count > rhs->count;
    });

    std::ofstream ofile(profiler_ctx->data_file, std::ios::out|std::ios::trunc);
    for (auto cs : arr) {
        ofile << cs->count;
        for (int i = cs->depth - 1; i >= 0; i--) {
            ofile << "|" << get_idname(cs->stack[i]);
        }
        ofile << std::endl;
    }

    ofile.close();
    FLOG_DEBUG("save call stack completed.");
    return 0;
}

static void dealloc_mapping(Mapping *map)
{
    int j = map->table_size;
    CallStack *elt, *nelt, **table = map->table;
    do {
        for (elt = table[j]; elt; elt = nelt) {
            nelt = elt->next;
            delete elt;
        }
    } while (j--);

    delete []table;
    FLOG_DEBUG("dealloc mapping.");
}

static void profiler_start()
{
    struct sigaction sa;
    sa.sa_sigaction = signal_profiler;
    sa.sa_flags = SA_RESTART | SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGPROF, &sa, NULL) != 0) {
        assert(0);
    }

    struct itimerval timer;
    timer.it_interval.tv_sec = 0;
    timer.it_interval.tv_usec = 1000;
    timer.it_value = timer.it_interval;
    setitimer(ITIMER_PROF, &timer, NULL);
}

static void profiler_stop()
{
    struct sigaction sa;
    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGPROF, &sa, NULL);

    struct itimerval timer;
    timer.it_interval.tv_sec = 0;
    timer.it_interval.tv_usec = 0;
    timer.it_value = timer.it_interval;
    setitimer(ITIMER_PROF, &timer, NULL);
}

static void backtrace(lua_State *L, lua_Debug *ar)
{
    if (ar->currentline >= 0) {
        if (*ar->namewhat != '\0') {
            lua_pushfstring(L, "%s@%d(%s)", ar->short_src, ar->linedefined, ar->name);
        } else if (*ar->what == 'm') {
            lua_pushfstring(L, "%s@--(main)", ar->short_src);
        } else if (*ar->what != 'C') {
            lua_pushfstring(L, "%s@%d(%s)", ar->short_src, ar->linedefined, ar->short_src);
        } else {
            lua_pushfstring(L, "%s:%d@?", ar->short_src, ar->currentline);
        }
    } else {
        if (*ar->namewhat != '\0') {
            lua_pushfstring(L, "%s@%s(%s)", ar->short_src, ar->namewhat, ar->name);
        } else if (*ar->what == 'm') {
            lua_pushfstring(L, "%s@--(main)", ar->short_src);
        } else if (*ar->what != 'C') {
            lua_pushfstring(L, "%s@function <%s:%d>", ar->short_src, ar->short_src, ar->linedefined);
        } else {
            lua_pushfstring(L, "%s@?", ar->short_src);
        }
    }
}

static void collect_callstack(lua_State *L)
{
    lua_Debug ar;
    int last = lastlevel(L);
    CallStack *cs = new CallStack();
    cs->depth = 0;

    while (lua_getstack(L, last--, &ar)) {
        if (lua_getinfo(L, "Slnt", &ar) < 0) continue;
        backtrace(L, &ar);
        // pushfuncname(L, &ar);
        const char *func_name = lua_tostring(L, -1);
        lua_pop(L, 1);

        int id = get_nameid(func_name);
        if (id <= 0) continue;

        cs->stack[cs->depth++] = id;
        FLOG_DEBUG("stack: %s %d %d", func_name, id, last);
    }

    mapping_insert(profiler_ctx->prof_data, cs);
}

static void signal_handler_hook(lua_State *L, lua_Debug *ar)
{
    FLOG_INFO("signal_handler_hook.");
    lua_sethook(L, NULL, 0, 0);
    if (profiler_ctx == NULL) return;

    CoroutineNode *cursor = profiler_ctx->tail;
    while (cursor) {
        collect_callstack(cursor->L);
        cursor = cursor->prev;
    }

    FLOG_INFO("restart profiler.");
    profiler_start();
}

static void signal_profiler(int sig, siginfo_t *sinfo, void *ucontext)
{
    FLOG_DEBUG("signal_profiler");
    profiler_stop();
    lua_sethook(profiler_ctx->tail->L, signal_handler_hook, LUA_MASKCOUNT, 1);
}

static CoroutineNode *link_co(ProfilerContext *ctx, lua_State *L)
{
    CoroutineNode *node = NULL;
    if (ctx->freelist) {
        node = ctx->freelist;
        ctx->freelist = node->next;
    } else {
        node = new CoroutineNode;
    }

    node->L = L;
    node->prev = NULL;
    node->next = NULL;

    if (ctx->head == NULL) {
        ctx->head = ctx->tail = node;
    } else {
        node->prev = ctx->tail;
        ctx->tail->next = node;
        ctx->tail = node;
    }

    FLOG_DEBUG("add new thread. L=%p", L);
    return node;
}

static void unlink_co(ProfilerContext *ctx, CoroutineNode *node)
{
    assert(node == ctx->tail);
    if (ctx->head == node) {
        ctx->head = ctx->tail = NULL;
    } else {
        ctx->tail = ctx->tail->prev;
        ctx->tail->next = NULL;
    }

    node->L = NULL;
    node->prev = NULL;

    node->next = ctx->freelist;
    ctx->freelist = node;
    FLOG_DEBUG("delete new thread. L=%p", node->L);
}

static int lresume(lua_State *L)
{
    ProfilerContext *ctx = (ProfilerContext *)lua_touserdata(L, lua_upvalueindex(2));

    lua_State *co = lua_tothread(L, 1);
    CoroutineNode *node = link_co(ctx, co);
    lua_CFunction co_resume = lua_tocfunction(L, lua_upvalueindex(1));
    int st = co_resume(L);
    unlink_co(ctx, node);
    return st;
}

static int lstop(lua_State *L)
{
    profiler_stop();

    lua_getglobal(L, "coroutine");
    lua_pushvalue(L, lua_upvalueindex(1));
    lua_setfield(L, -2, "resume");
    lua_pop(L, 1);

    ProfilerContext *ctx = profiler_ctx;

    mapping_save(ctx->prof_data);
    dealloc_mapping(ctx->prof_data);

    CoroutineNode *cursor = ctx->head;
    while (cursor) {
        CoroutineNode *tmp = cursor;
        cursor = cursor->next;
        delete tmp;
    }

    ctx->head = ctx->tail = NULL;

    cursor = ctx->freelist;
    while (cursor)
    {
        CoroutineNode *tmp = cursor;
        cursor = cursor->next;
        delete tmp;
    }

    ctx->freelist = NULL;

    if (ctx->log_fd > 0) {
        close(ctx->log_fd);
    }

    delete ctx;
    profiler_ctx = NULL;

    return 0;
}

static int lstart(lua_State *L)
{
    // if (L != G(L)->mainthread) {
    //     return luaL_error(L, "stack top only start in main thread.");
    // }

    init_nameid();

    size_t len = 0;
    const char *file = lua_tolstring(L, 1, &len);
    int debug_mode = luaL_optinteger(L, 2, 0);

    if (profiler_ctx == NULL) {
        profiler_ctx = new ProfilerContext;
        profiler_ctx->L = L;
        profiler_ctx->head = profiler_ctx->tail = NULL;
        profiler_ctx->debug_mode = debug_mode;
        profiler_ctx->freelist = NULL;
        profiler_ctx->prof_data = allocate_mapping(0);
        profiler_ctx->data_file = strdup(file);

        if (debug_mode) {
            profiler_ctx->log_fd = open("lperf.log", O_CREAT|O_WRONLY|O_APPEND, 0755);
            if (profiler_ctx->log_fd < 0) {
                return luaL_error(L, "cant open log file: %s.", strerror(errno));
            }
        }
    }

    lua_getglobal(L, "coroutine");
    lua_getfield(L, -1, "resume");
    lua_CFunction co_resume = lua_tocfunction(L, -1);
    lua_pushlightuserdata(L, profiler_ctx);
    lua_pushcclosure(L, lresume, 2);
    lua_setfield(L, -2, "resume");
    lua_pop(L, 1);

    lua_pushlightuserdata(L, profiler_ctx);
    lua_pushcfunction(L, co_resume);
    lua_pushcclosure(L, lstop, 2);

    link_co(profiler_ctx, L);
    profiler_start();

    return 1;
}

extern "C"
int luaopen_lperf(lua_State *L)
{
    luaL_checkversion(L);
    const luaL_Reg l[] = {
        {"start", lstart},
        {NULL, NULL},
    };

    luaL_newlib(L, l);
    return 1;
}