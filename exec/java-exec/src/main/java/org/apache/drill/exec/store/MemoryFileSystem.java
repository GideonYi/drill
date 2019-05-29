/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;


public class MemoryFileSystem extends FileSystem {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryFileSystem.class);

    static final String ERROR_MSG = "MemoryFileSystem is read only.";

    private Path working;

    private Map<String, Object> map = new HashMap<>();

    @Override
    public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
        throw new IOException(ERROR_MSG);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        throw new IOException(ERROR_MSG);
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public void setMap(Map<String, Object> map) {
        this.map = map;
    }

    @Override
    public boolean delete(Path arg0) throws IOException {
        throw new IOException(ERROR_MSG);
    }

    @Override
    public boolean delete(Path arg0, boolean arg1) throws IOException {
        throw new IOException(ERROR_MSG);
    }

    private String getFileName(Path path) {
        String file = path.toUri().getPath();
        if (file.charAt(0) == '/') {
            file = file.substring(1);
        }
        return file;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        //just available in memory
        return new FileStatus(0, false, 1, 1, 1, path);
    }

    @Override
    public URI getUri() {
        try {
            return new URI("memory:///");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Path getWorkingDirectory() {
        return working;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        throw new UnsupportedOperationException("MemoryFileSystem doesn't currently support listing files.");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission arg1) throws IOException {
        path.isAbsolute();
        return true;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        byte[] bytes;
        String file = getFileName(path);
        Object tableObj = map.get(file);
        bytes = tableObj.toString().getBytes();
        InputStream mis = new ResourceInputStream(bytes);
        return new FSDataInputStream(mis);
    }

    @Override
    public boolean rename(Path arg0, Path arg1) throws IOException {
        throw new IOException(ERROR_MSG);
    }

    @Override
    public void setWorkingDirectory(Path path) {
        this.working = path;
    }

    public static void main(String[] args) throws Exception {
        URI uri = new URI("memory:///");
    }
}
