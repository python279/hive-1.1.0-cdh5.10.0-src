/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DATABASE_WAREHOUSE_SUFFIX;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreFS;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This class represents a warehouse where data of Hive tables is stored
 */
public class Warehouse {
  private Path whRoot;
  private final Configuration conf;
  private final String whRootString;

  public static final Log LOG = LogFactory.getLog((String)"hive.metastore.warehouse");

  private MetaStoreFS fsHandler = null;
  private boolean storageAuthCheck = false;
  static final Pattern pat = Pattern.compile("([^/]+)=([^/]+)");
  private static final Pattern slash = Pattern.compile("/");

  public Warehouse(Configuration conf) throws MetaException {
    this.conf = conf;
    this.whRootString = HiveConf.getVar((Configuration)conf, (HiveConf.ConfVars)HiveConf.ConfVars.METASTOREWAREHOUSE);
    if (StringUtils.isBlank((String)this.whRootString)) {
      throw new MetaException(String.valueOf(HiveConf.ConfVars.METASTOREWAREHOUSE.varname) + " is not set in the config or blank");
    }
    this.fsHandler = this.getMetaStoreFsHandler(conf);
    this.storageAuthCheck = HiveConf.getBoolVar((Configuration)conf, (HiveConf.ConfVars)HiveConf.ConfVars.METASTORE_AUTHORIZATION_STORAGE_AUTH_CHECKS);
  }

  private MetaStoreFS getMetaStoreFsHandler(Configuration conf) throws MetaException {
    String handlerClassStr = HiveConf.getVar((Configuration)conf, (HiveConf.ConfVars)HiveConf.ConfVars.HIVE_METASTORE_FS_HANDLER_CLS);
    try {
      Class<?> handlerClass = Class.forName(handlerClassStr, true, JavaUtils.getClassLoader());
      MetaStoreFS handler = (MetaStoreFS)ReflectionUtils.newInstance(handlerClass, (Configuration)conf);
      return handler;
    }
    catch (ClassNotFoundException e) {
      throw new MetaException("Error in loading MetaStoreFS handler." + e.getMessage());
    }
  }


  /**
   * Helper functions to convert IOException to MetaException
   */
  public static FileSystem getFs(Path f, Configuration conf) throws MetaException {
    try {
      return f.getFileSystem(conf);
    }
    catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException((Exception)e);
      return null;
    }
  }

  public FileSystem getFs(Path f) throws MetaException {
    return Warehouse.getFs(f, this.conf);
  }

  public static void closeFs(FileSystem fs) throws MetaException {
    try {
      if (fs != null) {
        fs.close();
      }
    }
    catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException((Exception)e);
    }
  }


  /**
   * Hadoop File System reverse lookups paths with raw ip addresses The File
   * System URI always contains the canonical DNS name of the Namenode.
   * Subsequently, operations on paths with raw ip addresses cause an exception
   * since they don't match the file system URI.
   *
   * This routine solves this problem by replacing the scheme and authority of a
   * path with the scheme and authority of the FileSystem that it maps to.
   *
   * @param path
   *          Path to be canonicalized
   * @return Path with canonical scheme and authority
   */
  public static Path getDnsPath(Path path, Configuration conf) throws MetaException {
    FileSystem fs = Warehouse.getFs(path, conf);
    return new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(), path.toUri().getPath());
  }

  public Path getDnsPath(Path path) throws MetaException {
    return Warehouse.getDnsPath(path, this.conf);
  }

  /**
   * Resolve the configured warehouse root dir with respect to the configuration
   * This involves opening the FileSystem corresponding to the warehouse root
   * dir (but that should be ok given that this is only called during DDL
   * statements for non-external tables).
   */
  public Path getWhRoot() throws MetaException {
    if (this.whRoot != null) {
      return this.whRoot;
    }
    this.whRoot = this.getDnsPath(new Path(this.whRootString));
    return this.whRoot;
  }

  public Path getTablePath(String whRootString, String tableName) throws MetaException {
    Path whRoot = this.getDnsPath(new Path(whRootString));
    return new Path(whRoot, tableName.toLowerCase());
  }

  public Path getDatabasePath(Database db) throws MetaException {
    if (db.getName().equalsIgnoreCase(DEFAULT_DATABASE_NAME)) {
      return this.getWhRoot();
    }
    return new Path(db.getLocationUri());
  }

  public Path getDefaultDatabasePath(String dbName) throws MetaException {
    if (dbName.equalsIgnoreCase(DEFAULT_DATABASE_NAME)) {
      return this.getWhRoot();
    }
    return new Path(this.getWhRoot(), String.valueOf(dbName.toLowerCase()) + DATABASE_WAREHOUSE_SUFFIX);
  }

  public Path getTablePath(Database db, String tableName) throws MetaException {
    return this.getDnsPath(new Path(this.getDatabasePath(db), tableName.toLowerCase()));
  }

  public static String getQualifiedName(Table table) {
    return String.valueOf(table.getDbName()) + "." + table.getTableName();
  }

  public static String getQualifiedName(Partition partition) {
    return String.valueOf(partition.getDbName()) + "." + partition.getTableName() + partition.getValues();
  }

  public boolean mkdirs(Path f, boolean inheritPermCandidate) throws MetaException {
    boolean inheritPerms = HiveConf.getBoolVar((Configuration)this.conf, (HiveConf.ConfVars)HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS) && inheritPermCandidate;
    FileSystem fs = null;
    try {
      fs = this.getFs(f);
      return FileUtils.mkdir((FileSystem)fs, (Path)f, (boolean)inheritPerms, (Configuration)this.conf);
    }
    catch (IOException e) {
      Warehouse.closeFs(fs);
      MetaStoreUtils.logAndThrowMetaException((Exception)e);
      return false;
    }
  }

  public boolean renameDir(Path sourcePath, Path destPath) throws MetaException {
    return this.renameDir(sourcePath, destPath, false);
  }

  public boolean renameDir(Path sourcePath, Path destPath, boolean inheritPerms) throws MetaException {
    try {
      FileSystem fs = this.getFs(sourcePath);
      return FileUtils.renameWithPerms((FileSystem)fs, (Path)sourcePath, (Path)destPath, (boolean)inheritPerms, (Configuration)this.conf);
    }
    catch (Exception ex) {
      MetaStoreUtils.logAndThrowMetaException((Exception)ex);
      return false;
    }
  }

  public boolean deleteDir(Path f, boolean recursive) throws MetaException {
    return this.deleteDir(f, recursive, false);
  }

  public boolean deleteDir(Path f, boolean recursive, boolean ifPurge) throws MetaException {
    FileSystem fs = this.getFs(f);
    return this.fsHandler.deleteDir(fs, f, recursive, ifPurge, this.conf);
  }

  public boolean isEmpty(Path path) throws IOException, MetaException {
    ContentSummary contents = this.getFs(path).getContentSummary(path);
    if (contents != null && contents.getFileCount() == 0L && contents.getDirectoryCount() == 1L) {
      return true;
    }
    return false;
  }

  public boolean isWritable(Path path) throws IOException {
    if (!this.storageAuthCheck) {
      return true;
    }
    if (path == null) {
      return false;
    }
    try {
      FileSystem fs = this.getFs(path);
      FileStatus stat = fs.getFileStatus(path);
      ShimLoader.getHadoopShims().checkFileAccess(fs, stat, FsAction.WRITE);
      return true;
    }
    catch (FileNotFoundException fnfe) {
      return true;
    }
    catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug((Object)("Exception when checking if path (" + (Object)path + ")"), (Throwable)e);
      }
      return false;
    }
  }
  /*
  // NOTE: This is for generating the internal path name for partitions. Users
  // should always use the MetaStore API to get the path name for a partition.
  // Users should not directly take partition values and turn it into a path
  // name by themselves, because the logic below may change in the future.
  //
  // In the future, it's OK to add new chars to the escape list, and old data
  // won't be corrupt, because the full path name in metastore is stored.
  // In that case, Hive will continue to read the old data, but when it creates
  // new partitions, it will use new names.
  static BitSet charToEscape = new BitSet(128);
  static {
    for (char c = 0; c < ' '; c++) {
      charToEscape.set(c);
    }
    char[] clist = new char[] { '"', '#', '%', '\'', '*', '/', ':', '=', '?',
        '\\', '\u00FF' };
    for (char c : clist) {
      charToEscape.set(c);
    }
  }

  static boolean needsEscaping(char c) {
    return c >= 0 && c < charToEscape.size() && charToEscape.get(c);
  }
  */

  static String escapePathName(String path) {
    return FileUtils.escapePathName((String)path);
  }

  static String unescapePathName(String path) {
    return FileUtils.unescapePathName((String)path);
  }

  /**
   * Given a partition specification, return the path corresponding to the
   * partition spec. By default, the specification does not include dynamic partitions.
   * @param spec
   * @return string representation of the partition specification.
   * @throws MetaException
   */
  public static String makePartPath(Map<String, String> spec) throws MetaException {
    return Warehouse.makePartName(spec, true);
  }

  /**
   * Makes a partition name from a specification
   * @param spec
   * @param addTrailingSeperator if true, adds a trailing separator e.g. 'ds=1/'
   * @return partition name
   * @throws MetaException
   */
  public static String makePartName(Map<String, String> spec, boolean addTrailingSeperator) throws MetaException {
    StringBuilder suffixBuf = new StringBuilder();
    int i = 0;
    for (Map.Entry<String, String> e : spec.entrySet()) {
      if (e.getValue() == null || e.getValue().length() == 0) {
        throw new MetaException("Partition spec is incorrect. " + spec);
      }
      if (i>0) {
        suffixBuf.append(Path.SEPARATOR);
      }
      suffixBuf.append(Warehouse.escapePathName(e.getKey()));
      suffixBuf.append('=');
      suffixBuf.append(Warehouse.escapePathName(e.getValue()));
      ++i;
    }
    if (addTrailingSeperator) {
      suffixBuf.append(Path.SEPARATOR);
    }
    return suffixBuf.toString();
  }
  /**
   * Given a dynamic partition specification, return the path corresponding to the
   * static part of partition specification. This is basically a copy of makePartName
   * but we get rid of MetaException since it is not serializable.
   * @param spec
   * @return string representation of the static part of the partition specification.
   */
  public static String makeDynamicPartName(Map<String, String> spec) {
    StringBuilder suffixBuf = new StringBuilder();
    for (Map.Entry<String, String> e : spec.entrySet()) {
      if (e.getValue() == null || e.getValue().length() <= 0) break;
      suffixBuf.append(Warehouse.escapePathName(e.getKey()));
      suffixBuf.append('=');
      suffixBuf.append(Warehouse.escapePathName(e.getValue()));
      suffixBuf.append(Path.SEPARATOR);
    }
    return suffixBuf.toString();
  }

  /**
   * Extracts values from partition name without the column names.
   * @param name Partition name.
   * @param result The result. Must be pre-sized to the expected number of columns.
   */
  public static AbstractList<String> makeValsFromName( String name, AbstractList<String> result) throws MetaException {
    assert(name != null);
    String[] parts = slash.split(name, 0);
    if (result == null) {
      result = new ArrayList<String>(parts.length);
      for (int i = 0; i < parts.length; ++i) {
        result.add(null);
      }
    } else if (parts.length != result.size()) {
      throw new MetaException( "Expected " + result.size() + " components, got " + parts.length + " (" + name + ")");
    }
    for (int i = 0; i < parts.length; ++i) {
      int eq = parts[i].indexOf('=');
      if (eq <= 0) {
        throw new MetaException("Unexpected component " + parts[i]);
      }
      result.set(i, Warehouse.unescapePathName(parts[i].substring(eq + 1)));
    }
    return result;
  }

  public static LinkedHashMap<String, String> makeSpecFromName(String name) throws MetaException {
    if (name == null || name.isEmpty()) {
      throw new MetaException("Partition name is invalid. " + name);
    }
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
    Warehouse.makeSpecFromName(partSpec, new Path(name));
    return partSpec;
  }

  public static void makeSpecFromName(Map<String, String> partSpec, Path currPath) {
    ArrayList<String[]> kvs = new ArrayList<String[]>();
    do {
      String component;
      Matcher m;
      if (!(m = pat.matcher(component = currPath.getName())).matches()) continue;
      String k = Warehouse.unescapePathName(m.group(1));
      String v = Warehouse.unescapePathName(m.group(2));
      String[] kv = new String[]{k, v};
      kvs.add(kv);
    } while ((currPath = currPath.getParent()) != null && !currPath.getName().isEmpty());

    // reverse the list since we checked the part from leaf dir to table's base dir
    for (int i = kvs.size(); i > 0; --i) {
      partSpec.put(((String[])kvs.get(i - 1))[0], ((String[])kvs.get(i - 1))[1]);
    }
  }

  public static Map<String, String> makeEscSpecFromName(String name) throws MetaException {

    if (name == null || name.isEmpty()) {
      throw new MetaException("Partition name is invalid. " + name);
    }
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
    Path currPath = new Path(name);
    ArrayList<String[]> kvs = new ArrayList<String[]>();
    do {
      String component;
      Matcher m;
      if (!(m = pat.matcher(component = currPath.getName())).matches()) continue;
      String k = m.group(1);
      String v = m.group(2);
      String[] kv = new String[]{k, v};
      kvs.add(kv);
    } while ((currPath = currPath.getParent()) != null && !currPath.getName().isEmpty());
    // reverse the list since we checked the part from leaf dir to table's base dir
    for (int i = kvs.size(); i > 0; --i) {
      partSpec.put(((String[])kvs.get(i - 1))[0], ((String[])kvs.get(i - 1))[1]);
    }

    return partSpec;
  }

  public Path getPartitionPath(Database db, String tableName, LinkedHashMap<String, String> pm) throws MetaException {
    return new Path(this.getTablePath(db, tableName), Warehouse.makePartPath(pm));
  }

  public Path getPartitionPath(Path tblPath, LinkedHashMap<String, String> pm) throws MetaException {
    return new Path(tblPath, Warehouse.makePartPath(pm));
  }

  public boolean isDir(Path f) throws MetaException {
    FileSystem fs = null;
    try {
      fs = this.getFs(f);
      FileStatus fstatus = fs.getFileStatus(f);
      if (!fstatus.isDir()) {
        return false;
      }
    }
    catch (FileNotFoundException e) {
      return false;
    }
    catch (IOException e) {
      Warehouse.closeFs(fs);
      MetaStoreUtils.logAndThrowMetaException((Exception)e);
    }
    return true;
  }

  public static String makePartName(List<FieldSchema> partCols, List<String> vals) throws MetaException {
    return Warehouse.makePartName(partCols, vals, null);
  }

  /**
   * @param desc
   * @return array of FileStatus objects corresponding to the files
   * making up the passed storage description
   */
  public FileStatus[] getFileStatusesForSD(StorageDescriptor desc) throws MetaException {
    return this.getFileStatusesForLocation(desc.getLocation());
  }

  /**
   * @param location
   * @return array of FileStatus objects corresponding to the files
   * making up the passed storage description
   */
  public FileStatus[] getFileStatusesForLocation(String location) throws MetaException {
    try {
      Path path = new Path(location);
      FileSystem fileSys = path.getFileSystem(this.conf);
      return HiveStatsUtils.getFileStatusRecurse((Path)path, (int)-1, (FileSystem)fileSys);
    }
    catch (IOException ioe) {
      MetaStoreUtils.logAndThrowMetaException((Exception)ioe);
      return null;
    }
  }

  /**
   * @param table
   * @return array of FileStatus objects corresponding to the files making up the passed
   * unpartitioned table
   */
  public FileStatus[] getFileStatusesForUnpartitionedTable(Database db, Table table) throws MetaException {
    Path tablePath = this.getTablePath(db, table.getTableName());
    try {
      FileSystem fileSys = tablePath.getFileSystem(this.conf);
      return HiveStatsUtils.getFileStatusRecurse((Path)tablePath, (int)-1, (FileSystem)fileSys);
    }
    catch (IOException ioe) {
      MetaStoreUtils.logAndThrowMetaException((Exception)ioe);
      return null;
    }
  }

  /**
   * Makes a valid partition name.
   * @param partCols The partition columns
   * @param vals The partition values
   * @param defaultStr
   *    The default name given to a partition value if the respective value is empty or null.
   * @return An escaped, valid partition name.
   * @throws MetaException
   */
  public static String makePartName(List<FieldSchema> partCols, List<String> vals, String defaultStr) throws MetaException {
    if (partCols.size() != vals.size() || partCols.size() == 0) {
      String errorStr = "Invalid partition key & values; keys [";
      for (FieldSchema fs : partCols) {
        errorStr = String.valueOf(errorStr) + fs.getName() + ", ";
      }
      errorStr = String.valueOf(errorStr) + "], values [";
      for (String val : vals) {
        errorStr = String.valueOf(errorStr) + val + ", ";
      }
      throw new MetaException(String.valueOf(errorStr) + "]");
    }
    ArrayList<String> colNames = new ArrayList<String>();
    for (FieldSchema col: partCols) {
      colNames.add(col.getName());
    }
    return FileUtils.makePartName(colNames, vals, (String)defaultStr);
  }

  public static List<String> getPartValuesFromPartName(String partName) throws MetaException {
    LinkedHashMap<String, String> partSpec = Warehouse.makeSpecFromName(partName);
    ArrayList<String> values = new ArrayList<String>();
    values.addAll(partSpec.values());
    return values;
  }

  public static Map<String, String> makeSpecFromValues(List<FieldSchema> partCols, List<String> values) {
    LinkedHashMap<String, String> spec = new LinkedHashMap<String, String>();
    int i = 0;
    while (i < values.size()) {
      spec.put(partCols.get(i).getName(), values.get(i));
      ++i;
    }
    return spec;
  }
}
