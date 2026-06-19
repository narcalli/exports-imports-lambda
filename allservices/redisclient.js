const { createClient } = require("redis");

/* ------------------------------------------------------------------ */
/*                         RAW REDIS CLIENT                            */
/* ------------------------------------------------------------------ */

const _redisClient = createClient({
  url: `redis://default:${process.env.REDIS_P}@${process.env.REDIS_U}:${process.env.REDIS_R}`,
  socket: {
    keepAlive: 5000,
    connectTimeout: 10000,
    reconnectStrategy: (retries) => Math.min(retries * 100, 3000)
  },
});

/**
 * Internal flags
 * _blocked = true → do not allow commands
 */
_redisClient._blocked = true;

/* ------------------------- lifecycle events ------------------------ */

_redisClient.on("ready", () => {
  _redisClient._blocked = false;

  const date = new Date()

  console.log(`[${date.toISOString()}]: Redis ready`);
});

_redisClient.on("reconnecting", () => {
  _redisClient._blocked = true;

  const date = new Date()
  console.warn(`[${date.toISOString()}]: Redis reconnecting`);
});

_redisClient.on("end", () => {
  _redisClient._blocked = true;
  console.warn("Redis connection ended");
});

_redisClient.on("error", (err) => {
  const date = new Date()
  console.error(`[${date.toISOString()}]: Redis error, `, err.message);
});

/* ---------------------------- connect ------------------------------ */

(async () => {
  try {
    await _redisClient.connect();
  } catch (err) {
    console.error("Redis initial connect failed:", err.message);
  }
})();

/* --------------------------- heartbeat ----------------------------- */

setInterval(async () => {
  try {
    if (_redisClient.isReady) {
      await _redisClient.ping();
    }
  } catch {
    // ignore heartbeat errors
  }
}, 60_000);

/* ------------------------------------------------------------------ */
/*                         REDIS SERVICE                               */
/* ------------------------------------------------------------------ */

class RedisService {
  /**
   * Wait until redis is ready or timeout
   */
  async waitForReady(timeoutMs = 400) {
    if (_redisClient.isReady && !_redisClient._blocked) {
      return true;
    }

    const start = Date.now();

    return new Promise((resolve) => {
      const interval = setInterval(() => {
        if (_redisClient.isReady && !_redisClient._blocked) {
          clearInterval(interval);
          return resolve(true);
        }

        if (Date.now() - start >= timeoutMs) {
          clearInterval(interval);
          resolve(false);
        }
      }, 25);
    });
  }

  /**
   * Generic executor
   */
  async exec(fn, fallback = null) {
    const ready = await this.waitForReady();
    if (!ready) return fallback;

    try {
      return await fn();
    } catch (err) {
      return fallback;
    }
  }

  /* -------------------- basic redis commands -------------------- */

  get(key) {
    return this.exec(() => _redisClient.get(key));
  }

  set(key, value, opts) {
    return this.exec(() => _redisClient.set(key, value, opts));
  }

  del(key) {
    return this.exec(() => _redisClient.del(key));
  }

  exists(key) {
    return this.exec(() => _redisClient.exists(key), 0);
  }

  hGet(key, field) {
    return this.exec(() => _redisClient.hGet(key, field), null);
  }

  hGetAll(key) {
    return this.exec(() => _redisClient.hGetAll(key), {});
  }

  hSet(key, fieldOrMap, value) {
    // supports both:
    // hSet(key, field, value)
    // hSet(key, { field1: val1, field2: val2 })

    if (typeof fieldOrMap === "object" && value === undefined) {
      // object form
      return this.exec(
        () => _redisClient.hSet(key, fieldOrMap, value || {}),
        0
      );
    }

    // single field form
    return this.exec(() => _redisClient.hSet(key, fieldOrMap, value), 0);
  }

  pExpire(key, milliseconds) {
    return this.exec(() => _redisClient.pExpire(key, milliseconds), false);
  }

  hDel(key, field) {
    return this.exec(() => _redisClient.hDel(key, field), 0);
  }

  lRange(key, start, stop) {
    return this.exec(() => _redisClient.lRange(key, start, stop), []);
  }

  lRem(poolKey, count, args) {
    return this.exec(() => _redisClient.lRem(poolKey, count, args));
  }

  sMembers(key) {
    return this.exec(() => _redisClient.sMembers(key), []);
  }

  sCard(key) {
    return this.exec(() => _redisClient.sCard(key), 0);
  }

  sendCommand(commandArgs) {
    return this.exec(() => _redisClient.sendCommand(commandArgs), []);
  }

  ttl(key) {
    return this.exec(() => _redisClient.ttl(key));
  }

  expire(key, seconds) {
    return this.exec(() => _redisClient.expire(key, seconds));
  }

  rename(oldKey, newKey) {
    return this.exec(() => _redisClient.rename(oldKey, newKey), false);
  }

  zAdd(key, members) {
    return this.exec(() => _redisClient.zAdd(key, members), false);
  }

  zRem(key, member) {
    return this.exec(() => _redisClient.zRem(key, member), false);
  }

  rPush(key, element) {
    return this.exec(() => _redisClient.rPush(key, element), false);
  }

  keys(pattern) {
    return this.exec(() => _redisClient.keys(pattern), []);
  }

  type(key) {
    return this.exec(() => _redisClient.type(key));
  }

  /* -------------------- MULTI / PIPELINE -------------------- */

  multi() {
    const pipeline = _redisClient.multi();
    const service = this;

    const wrapper = {
      get(key) {
        pipeline.get(key);
        return wrapper;
      },

      hGetAll(key) {
        pipeline.hGetAll(key);
        return wrapper;
      },

      lRange(key, start, stop) {
        pipeline.lRange(key, start, stop);
        return wrapper;
      },

      del(key) {
        pipeline.del(key);
        return wrapper;
      },

      expire(key, seconds) {
        pipeline.expire(key, seconds);
        return wrapper;
      },

      zAdd(key, members) {
        return pipeline.zAdd(key, members);
      },


      pExpire(key, milliseconds) {
        pipeline.pExpire(key, milliseconds);
        return wrapper;
      },

      zRem(key, member) {
        return pipeline.zRem(key, member);
      },

      rPush(key, element) {
        return pipeline.rPush(key, element);
      },

      hSet(key, fieldOrMap, value) {
        if (typeof fieldOrMap === "object" && value === undefined) {
          // object form
          return pipeline.hSet(key, fieldOrMap, value || {});
        }

        // single field form
        return pipeline.hSet(key, fieldOrMap, value);
      },

      json: {
        set(key, path, value) {
          pipeline.json.set(key, path, value);
          return wrapper;
        },
      },

      async exec() {
        const ready = await service.waitForReady();
        if (!ready) return [];

        try {
          return await pipeline.exec();
        } catch {
          return [];
        }
      },
    };

    return wrapper;
  }

  json = {
    get: (key, path = "$") => {
      return this.exec(() => _redisClient.json.get(key, { path }), null);
    },

    set: (key, path, value) => {
      return this.exec(() => _redisClient.json.set(key, path, value), false);
    },

    del: (key, path = "$") => {
      return this.exec(() => _redisClient.json.del(key, path), 0);
    },

    arrAppend: (key, path = "$", value) => {
      return this.exec(
        () => _redisClient.json.arrAppend(key, path, value),
        false
      );
    },

    objKeys: (key) => {
      return this.exec(() => _redisClient.json.objKeys(key), []);
    },
  };

  ft = {
    search: (index, query, options = {}) => {
      return this.exec(() => _redisClient.ft.search(index, query, options), {
        total: 0,
        documents: [],
      });
    },

    aggregate: (index, query, options = {}) => {
      return this.exec(() => _redisClient.ft.aggregate(index, query, options), {
        total: 0,
        results: [],
      });
    },

    info: (index) => {
      return this.exec(() => _redisClient.ft.info(index), {});
    },
    create: (index, schema, options) => {
      return this.exec(() => _redisClient.ft.create(index, schema, options));
    },
    dropIndex: (index) => {
      return this.exec(() => _redisClient.ft.dropIndex(index));
    },
    _list: () => {
      return this.exec(() => _redisClient.ft._list());
    },
  };
}

/* ------------------------------------------------------------------ */

module.exports = _redisClient

// const { createClient } = require("redis");

// const redisClient = createClient({
//   url: `redis://default:${process.env.REDIS_P}@${process.env.REDIS_U}:${process.env.REDIS_R}`,
//   socket: {
//     reconnectStrategy: retries => Math.min(retries * 100, 3000),
//     keepAlive: 5000,        // 🔥 IMPORTANT
//     connectTimeout: 10000   // avoids hanging connects
//   }
// });

// redisClient.on("connect", () => {
//   console.log("Redis connected");
// });

// redisClient.on("reconnecting", () => {
//   console.log("Redis reconnecting");
// });

// redisClient.on("error", (err) => {
//   console.error("Redis error:", err.message, err?.stack);
// });

// redisClient.on("end", () => {
//   console.warn("Redis connection ended");
// });

// (async () => {
//   try {
//     await redisClient.connect();
//   } catch (err) {
//     console.error("Redis connection failed:", err.message);
//   }
// })();

// module.exports = redisClient;

// const Promise = require("bluebird");
// // const redis = require('redis');
// const {
//   createClient,
//   SchemaFieldTypes,
//   AggregateGroupByReducers,
//   AggregateSteps,
// } = require("redis");
// const config = require("../config");

// // Promise.promisifyAll(require("redis"));
// // Promise.promisifyAll(redis.RedisClient.prototype);
// // Promise.promisifyAll(redis.Multi.prototype);

// // sep 28 : migrating to redislabs on heroku, but not using process.env variable

// /**
//  * @type {import("redis").RedisClientType}
//  */
// let redisClient = ``;

// (async function triggerGatePassFlow() {
//   redisClient = createClient({

//     // url: `redis://default:B8AAVsCTieA4eDacrKssZXGHptySKFFB@redis-12014.c281.us-east-1-2.ec2.cloud.redislabs.com:12014`,
//     url: `redis://default:${process.env.REDIS_P}@${process.env.REDIS_U}:${process.env.REDIS_R}`,
//     socket: {
//       reconnectStrategy: retries => Math.min(retries * 50, 2000)
//     }
//   });

//   await redisClient
//     .connect()
//     // .then((err) => err && console.log({ err_redis_connect_26_err: err }));
// })();

// redisClient.on("connect", function () {
//   console.log("Redis connected");
// });

// redisClient.on("reconnecting", function () {
//   console.log("Redis reconnecting -- line 32");
// });

// redisClient.on("error", function (err) {
//   console.log({ redisclientconnect_40: err.message }, err?.stack);
// });

// redisClient.on("end", () => {
//   console.warn("Redis connection ended");
// });

// module.exports = redisClient;
