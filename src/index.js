const path = require('path');
const fs = require('fs/promises');
const crypto = require('crypto');

module.exports = class BlobbyFS {
  constructor(opts) {
    this.options = opts || {};
    if (!this.options.path) throw new Error('BlobbyFS requires `path` option');
  }

  fetchInfo(fileKey, opts = {}) {
    const absPath = path.resolve(path.join(this.options.path, fileKey));
    return fs.stat(absPath).then(stats => {
      if (!stats.isFile()) throw new Error('Requested path is not a file');

      return { LastModified: stats.mtime, Size: stats.size };
    });
  }

  /*
    fileKey: unique id for storage
    opts: future
   */
  async fetch(fileKey) {
    const absPath = path.resolve(path.join(this.options.path, fileKey));
    const data = await fs.readFile(absPath, { encoding: null });

    // compute etag
    const ETag = crypto.createHash('md5').update(data).digest('hex');

    const stats = await fs.stat(absPath);

    // tuple contains headers and data
    return [{ ETag, LastModified: stats.mtime, Size: stats.size }, data];
  }

  /*
   fileKey: unique id for storage
   file: file object
   file.buffer: Buffer containing file data
   file.headers: A collection of header values
   file.headers.LastModified: If specified, will force this value on newly written files
   opts: future
   */
  async store(fileKey, file, opts = {}) {
    const { buffer, headers = {} } = file;
    const absPath = path.resolve(path.join(this.options.path, fileKey));
    const $this = this;
    await fs.writeFile(absPath, buffer, {}).catch(async err => {
      if (err.code === 'ENOENT') {
        await fs.mkdir(path.dirname(absPath), { recursive: true });

        // success, so lets try to store again
        return $this.store(fileKey, file, opts);
      } else {
        throw err;
      }
    });

    // compute etag
    headers.ETag = crypto.createHash('md5').update(buffer).digest('hex');

    if (typeof headers.LastModified === 'object') {
      // if LastModified is set, apply to target object for proper syncing
      await fs.utimes(absPath,
        Date.now() / 1000 /* atime: Access Time */,
        headers.LastModified.getTime() / 1000 /* mtime: Modified Time */
      );
    }
    return headers;
  }

  /*
   fileKey: unique id for storage
   */
  remove(fileKey) {
    const absPath = path.resolve(path.join(this.options.path, fileKey));
    return fs.unlink(absPath);
  }

  /*
   dir: unique id for storage
   */
  removeDirectory(dir) {
    const absPath = path.resolve(path.join(this.options.path, dir)) + '/';
    return fs.rmdir(absPath, { recursive: true });
  }

  /* supported options:
   dir: Directory (prefix) to query
   opts: Options object
   opts.lastKey: if requesting beyond maxKeys (paging)
   opts.maxKeys: ignored for this storage client
   opts.deepQuery: not supported for this storage client
  */
  list(dir, opts = {}) {
    if (!opts.deepQuery) {
      // simple mode, query current directory

      return query(this.options.path, dir);
    }

    // otherwise use deep query logic for use with querying entire tree
    return deepQuery(this.options.path, dir, opts.lastKey);
  }
}

async function query(root, currentDir, opts = {}) {
  opts.lastDir = opts.lastDir || '';
  const absPath = path.join(root, currentDir);
  const dirContents = await fs.readdir(absPath);

  const dirs = [];
  const files = [];
  for (let name of dirContents) {
    const Key = path.join(currentDir, name);
    if (name <= opts.lastDir) {
      // Ignore/filter anything less than or equal to lastDir.
      // This is not only an optimization, but is a required pattern
      // to avoid returning the same file more than once.
      continue;
    }
    const stats = await fs.stat(path.join(root, Key));
    if (stats.isDirectory()) {
      dirs.push({ Key });
      continue;
    }

    if (!opts.ignoreFiles) {
      files.push({ Key, LastModified: stats.mtime, Size: stats.size });
    }
  }

  return [files.sort(sortByKey), dirs.sort(sortByKey)];
}

function sortByKey(a, b) {
  if (a.Key > b.Key) return 1; // greater than
  else if (a.Key < b.Key) return -1; // less than
  return 0; // equal
}

/*
  lastKey = the next directory to query, in lexical order

  // example structure:
  a/b/c/d
  a/b/x
  a/b/y
  a/e
  a/f

  # lastKey format: (+right, -left){lastDir}:{nextDir}
  +:a
  +a:a/b
  +a/b:/a/b/c
  +a/b/c:a/b/c/d
  -a/b/c/d:a/b/c
  -a/b/x:a/b
  -a/b/y:a/b
  -a/e:a
  -a/f:a
  -a:.
  (eof, no more dirs after `a`)
*/
async function deepQuery(root, currentDir, lastKey) {
  if (!lastKey) {
    // initial query
    return query(root, currentDir).then(([files, dirs]) => {
      const lastKey = dirs.length > 0 ? buildLastKeyRight(currentDir, path.join(currentDir, dirs[0].Key)) : null;
      return [files, [], lastKey];
    });
  }

  // resume query via lastKey
  const keyInfo = getLastKeyInfo(lastKey);
  const lastDir = keyInfo.leftToRight === false ? path.basename(keyInfo.lastDir) : null;
  const ignoreFiles = !keyInfo.leftToRight; // ignore files in nextDir if going backwards
  //console.log(`querying ${keyInfo.nextDir}, leftToRight:${keyInfo.leftToRight}, lastDir:${lastDir}, ignoreFiles:${ignoreFiles}...`)
  const [files, dirs] = await query(root, keyInfo.nextDir, { lastDir, ignoreFiles });

  if (dirs.length === 0) {
    // no directories to continue down, go back
    return [files, [], buildLastKeyLeft(keyInfo.nextDir)];
  } else {
    // contains directories, so continue searching
    return [files, [], buildLastKeyRight(keyInfo.nextDir, dirs[0].Key)];
  }
}

function getLastKeyInfo(lastKey) {
  const leftToRight = lastKey[0] === '+';
  const split = lastKey.substr(1).split(':');
  const lastDir = split[0];
  const nextDir = split[1];
  return {
    leftToRight,
    lastDir,
    nextDir
  };
}

function buildLastKeyRight(currentDir, nextDir) {
  return `+${currentDir}:${nextDir}`;
}

function buildLastKeyLeft(currentDir) {
  const nextDir = path.join(currentDir, '..');
  if (nextDir === '..') return null; // no key if in root and we go back again
  return `-${currentDir}:${nextDir}`;
}
