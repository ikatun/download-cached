const path = require('path');
const crypto = require('crypto');
const fs = require('fs');
const uuid = require('uuid/v1');
const mkdirp = require('mkdirp-promise');

function readStreamToPromise(readStream) {
  return new Promise((resolve, reject) => {
    readStream.on('error', err => reject(err));
    readStream.on('open', () => resolve(readStream));
  });
}

function getCachedFilePath(cacheDirectory, url) {
  const urlHash = crypto.createHash('md5').update(url).digest('hex');
  return path.join(cacheDirectory, urlHash);
}

function httpGet(url) {
  const client = typeof url === 'string' && url.startsWith('https') ? require('https') : require('http');
  return new Promise((resolve, reject) => {
      client.get(url, res => {
        if (res.statusCode !== 200) {
          reject(new Error('Response status code is ' + res.statusCode));
        } else {
          resolve(res);
        }
      }).on('error', err => reject(err));
    })
}

function fetchGet(url) {
  return fetch(url)
    .then(res => {
      if (res.status === 200) {
        return res.body;
      } else {
        return Promise.reject(new Error('Response status code is ' + res.status));
      }
    })
}

function requestGet(url) {
  return new Promise((resolve, reject) => {
    const req = require('request')(url);
    req.on('response', res => {
      if (res.statusCode !== 200) {
        reject(new Error('Response status code is ' + res.statusCode));
      } else {
        res.pause();
        resolve(res);
      }
    })
    .on('error', err => reject(err));
  })
}

module.exports = (cacheDirectory, urlToStreamPromise = httpGet) => {

  const pendingCacheDirectory = path.join(cacheDirectory, 'pending');
  const createCacheDirPromise = mkdirp(pendingCacheDirectory);

  const downloadToStream = url => mkdirp(pendingCacheDirectory).then(() => {
    const cachedFilePath = getCachedFilePath(cacheDirectory, url)
    // if file is already cached, return stream from cache
    return readStreamToPromise(fs.createReadStream(cachedFilePath));
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
  
  const download = url => {
    const downloadPromise = downloadToStream(url);
    
    downloadPromise.toFile = destPath => downloadPromise.then(sourceStream => {
      const destStream = fs.createWriteStream(destPath);
      sourceStream.pipe(destStream);
      return new Promise((resolve, reject) => {
        destStream.on('close', () => resolve());
        destStream.on('error', err => reject(err));
      });
    });

    return downloadPromise;
  }

  download.clear = url => {
    const filePath = getCachedFilePath(cacheDirectory, url);
    return new Promise((resolve, reject) => {
      fs.unlink(filePath, err => err && err.code !== 'ENOENT' ? reject(err) : resolve());
    });
  };

  return download;
}

module.exports.httpGet = httpGet;
module.exports.requestGet = requestGet;
module.exports.fetchGet = fetchGet;
