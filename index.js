const path = require('path');
const crypto = require('crypto');
const fs = require('fs');
const uuid = require('uuid/v1');
const mkdirp = require('mkdirp-promise');

function getCachedFilePath(cacheDirectory, url) {
  const urlHash = crypto.createHash('md5').update(url).digest('hex');
  return path.join(cacheDirectory, urlHash);
}

function asPromise(f, ...args) {
  return new Promise((resolve, reject) => {
    f(...args, (err, ...res) => err ? reject(err) : resolve(res.length < 2 ? res[0] : res));
  });
}

function promiseEvent(emitter, event) {
  return new Promise((resolve, reject) => {
    emitter.on(event, res => resolve(emitter));
    emitter.on('error', err => reject(err));
  });
}

function httpGet(url) {
  const client = typeof url === 'string' && url.startsWith('https') ? require('https') : require('http');
  return new Promise((resolve, reject) => {
      client.get(url, res => {
        if (res.statusCode !== 200) {
          reject(new Error('Response status code is ' + res.statusCode));
        } else {
          res.contentLength = res.headers['content-length'] || null;
          resolve(res);
        }
      }).on('error', err => reject(err));
    });
}

const fetchGet = fetch => url => fetch(url)
  .then(res => {
    if (res.status === 200) {
      res.body.contentLength = res.headers.get('content-length') || null;
      return res.body;
    } else {
      return Promise.reject(new Error('Response status code is ' + res.status));
    }
  });

const requestGet = request => url => new Promise((resolve, reject) => {
  const req = request(url);
  req.on('response', res => {
    if (res.statusCode !== 200) {
      reject(new Error('Response status code is ' + res.statusCode));
    } else {
      res.contentLength = res.headers['content-length'] || null;
      res.pause();
      resolve(res);
    }
  })
  .on('error', err => reject(err));
});

function writeStreamToFile(srcStream, destPath) {
  const destStream = fs.createWriteStream(destPath);
  srcStream.pipe(destStream);
  return promiseEvent(destStream, 'close');
}

function openFsReadStreamWithSize(path) {
  return Promise.all([
    asPromise(fs.stat, path),
    promiseEvent(fs.createReadStream(path), 'open')
  ]).then(([stats, readStream]) => {
    readStream.contentLength = stats.size;
    return readStream;
  })
}

module.exports = (cacheDirectory, urlToStreamPromise = httpGet) => {
  const pendingCacheDirectory = path.join(cacheDirectory, 'pending');
  const createCacheDirPromise = mkdirp(pendingCacheDirectory);

  const download = url => mkdirp(pendingCacheDirectory).then(() => {
    const cachedFilePath = getCachedFilePath(cacheDirectory, url);
    // if file is already cached, return stream from cache
    return openFsReadStreamWithSize(cachedFilePath);
  }).catch(err => {
    if (err.code !== 'ENOENT') {
      return Promise.reject(err);
    }
    return urlToStreamPromise(url);
  }).then(networkStream => {
    // file is not cached, start downloading into a pending directory
    const pendingFilePath = path.join(pendingCacheDirectory, uuid());
    const pendingWriteStream = fs.createWriteStream(pendingFilePath);
    networkStream.pipe(pendingWriteStream);
    pendingWriteStream.on('close', () => {
      // file downloaded, move it from the pending directory to the cache directory
      fs.rename(pendingFilePath, getCachedFilePath(cacheDirectory, url));
    });
    pendingWriteStream.on('error', () => {
      fs.unlink(pendingFilePath, () => {});
    });
    return networkStream;
  });

  download.clear = url => {
    const filePath = getCachedFilePath(cacheDirectory, url);
    return asPromise(fs.unlink, filePath).catch(err => err.code === 'ENOENT' ? Promise.resolve() : Promise.reject(err));
  };

  download.toFile = (url, destPath) => download(url).then(stream => writeStreamToFile(stream, destPath));

  return download;
}

module.exports.httpGet = httpGet;
module.exports.requestGet = requestGet;
module.exports.fetchGet = fetchGet;
