/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionsByExprRequest implements org.apache.thrift.TBase<PartitionsByExprRequest, PartitionsByExprRequest._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("PartitionsByExprRequest");

  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("dbName", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TBL_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("tblName", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField EXPR_FIELD_DESC = new org.apache.thrift.protocol.TField("expr", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField DEFAULT_PARTITION_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("defaultPartitionName", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField MAX_PARTS_FIELD_DESC = new org.apache.thrift.protocol.TField("maxParts", org.apache.thrift.protocol.TType.I16, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new PartitionsByExprRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new PartitionsByExprRequestTupleSchemeFactory());
  }

  private String dbName; // required
  private String tblName; // required
  private ByteBuffer expr; // required
  private String defaultPartitionName; // optional
  private short maxParts; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DB_NAME((short)1, "dbName"),
    TBL_NAME((short)2, "tblName"),
    EXPR((short)3, "expr"),
    DEFAULT_PARTITION_NAME((short)4, "defaultPartitionName"),
    MAX_PARTS((short)5, "maxParts");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // DB_NAME
          return DB_NAME;
        case 2: // TBL_NAME
          return TBL_NAME;
        case 3: // EXPR
          return EXPR;
        case 4: // DEFAULT_PARTITION_NAME
          return DEFAULT_PARTITION_NAME;
        case 5: // MAX_PARTS
          return MAX_PARTS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __MAXPARTS_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private _Fields optionals[] = {_Fields.DEFAULT_PARTITION_NAME,_Fields.MAX_PARTS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.DB_NAME, new org.apache.thrift.meta_data.FieldMetaData("dbName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TBL_NAME, new org.apache.thrift.meta_data.FieldMetaData("tblName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.EXPR, new org.apache.thrift.meta_data.FieldMetaData("expr", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.DEFAULT_PARTITION_NAME, new org.apache.thrift.meta_data.FieldMetaData("defaultPartitionName", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.MAX_PARTS, new org.apache.thrift.meta_data.FieldMetaData("maxParts", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I16)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(PartitionsByExprRequest.class, metaDataMap);
  }

  public PartitionsByExprRequest() {
    this.maxParts = (short)-1;

  }

  public PartitionsByExprRequest(
    String dbName,
    String tblName,
    ByteBuffer expr)
  {
    this();
    this.dbName = dbName;
    this.tblName = tblName;
    this.expr = expr;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public PartitionsByExprRequest(PartitionsByExprRequest other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetTblName()) {
      this.tblName = other.tblName;
    }
    if (other.isSetExpr()) {
      this.expr = org.apache.thrift.TBaseHelper.copyBinary(other.expr);
;
    }
    if (other.isSetDefaultPartitionName()) {
      this.defaultPartitionName = other.defaultPartitionName;
    }
    this.maxParts = other.maxParts;
  }

  public PartitionsByExprRequest deepCopy() {
    return new PartitionsByExprRequest(this);
  }

  @Override
  public void clear() {
    this.dbName = null;
    this.tblName = null;
    this.expr = null;
    this.defaultPartitionName = null;
    this.maxParts = (short)-1;

  }

  public String getDbName() {
    return this.dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void unsetDbName() {
    this.dbName = null;
  }

  /** Returns true if field dbName is set (has been assigned a value) and false otherwise */
  public boolean isSetDbName() {
    return this.dbName != null;
  }

  public void setDbNameIsSet(boolean value) {
    if (!value) {
      this.dbName = null;
    }
  }

  public String getTblName() {
    return this.tblName;
  }

  public void setTblName(String tblName) {
    this.tblName = tblName;
  }

  public void unsetTblName() {
    this.tblName = null;
  }

  /** Returns true if field tblName is set (has been assigned a value) and false otherwise */
  public boolean isSetTblName() {
    return this.tblName != null;
  }

  public void setTblNameIsSet(boolean value) {
    if (!value) {
      this.tblName = null;
    }
  }

  public byte[] getExpr() {
    setExpr(org.apache.thrift.TBaseHelper.rightSize(expr));
    return expr == null ? null : expr.array();
  }

  public ByteBuffer bufferForExpr() {
    return expr;
  }

  public void setExpr(byte[] expr) {
    setExpr(expr == null ? (ByteBuffer)null : ByteBuffer.wrap(expr));
  }

  public void setExpr(ByteBuffer expr) {
    this.expr = expr;
  }

  public void unsetExpr() {
    this.expr = null;
  }

  /** Returns true if field expr is set (has been assigned a value) and false otherwise */
  public boolean isSetExpr() {
    return this.expr != null;
  }

  public void setExprIsSet(boolean value) {
    if (!value) {
      this.expr = null;
    }
  }

  public String getDefaultPartitionName() {
    return this.defaultPartitionName;
  }

  public void setDefaultPartitionName(String defaultPartitionName) {
    this.defaultPartitionName = defaultPartitionName;
  }

  public void unsetDefaultPartitionName() {
    this.defaultPartitionName = null;
  }

  /** Returns true if field defaultPartitionName is set (has been assigned a value) and false otherwise */
  public boolean isSetDefaultPartitionName() {
    return this.defaultPartitionName != null;
  }

  public void setDefaultPartitionNameIsSet(boolean value) {
    if (!value) {
      this.defaultPartitionName = null;
    }
  }

  public short getMaxParts() {
    return this.maxParts;
  }

  public void setMaxParts(short maxParts) {
    this.maxParts = maxParts;
    setMaxPartsIsSet(true);
  }

  public void unsetMaxParts() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __MAXPARTS_ISSET_ID);
  }

  /** Returns true if field maxParts is set (has been assigned a value) and false otherwise */
  public boolean isSetMaxParts() {
    return EncodingUtils.testBit(__isset_bitfield, __MAXPARTS_ISSET_ID);
  }

  public void setMaxPartsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __MAXPARTS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case DB_NAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String)value);
      }
      break;

    case TBL_NAME:
      if (value == null) {
        unsetTblName();
      } else {
        setTblName((String)value);
      }
      break;

    case EXPR:
      if (value == null) {
        unsetExpr();
      } else {
        setExpr((ByteBuffer)value);
      }
      break;

    case DEFAULT_PARTITION_NAME:
      if (value == null) {
        unsetDefaultPartitionName();
      } else {
        setDefaultPartitionName((String)value);
      }
      break;

    case MAX_PARTS:
      if (value == null) {
        unsetMaxParts();
      } else {
        setMaxParts((Short)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case DB_NAME:
      return getDbName();

    case TBL_NAME:
      return getTblName();

    case EXPR:
      return getExpr();

    case DEFAULT_PARTITION_NAME:
      return getDefaultPartitionName();

    case MAX_PARTS:
      return Short.valueOf(getMaxParts());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case DB_NAME:
      return isSetDbName();
    case TBL_NAME:
      return isSetTblName();
    case EXPR:
      return isSetExpr();
    case DEFAULT_PARTITION_NAME:
      return isSetDefaultPartitionName();
    case MAX_PARTS:
      return isSetMaxParts();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof PartitionsByExprRequest)
      return this.equals((PartitionsByExprRequest)that);
    return false;
  }

  public boolean equals(PartitionsByExprRequest that) {
    if (that == null)
      return false;

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
        return false;
    }

    boolean this_present_tblName = true && this.isSetTblName();
    boolean that_present_tblName = true && that.isSetTblName();
    if (this_present_tblName || that_present_tblName) {
      if (!(this_present_tblName && that_present_tblName))
        return false;
      if (!this.tblName.equals(that.tblName))
        return false;
    }

    boolean this_present_expr = true && this.isSetExpr();
    boolean that_present_expr = true && that.isSetExpr();
    if (this_present_expr || that_present_expr) {
      if (!(this_present_expr && that_present_expr))
        return false;
      if (!this.expr.equals(that.expr))
        return false;
    }

    boolean this_present_defaultPartitionName = true && this.isSetDefaultPartitionName();
    boolean that_present_defaultPartitionName = true && that.isSetDefaultPartitionName();
    if (this_present_defaultPartitionName || that_present_defaultPartitionName) {
      if (!(this_present_defaultPartitionName && that_present_defaultPartitionName))
        return false;
      if (!this.defaultPartitionName.equals(that.defaultPartitionName))
        return false;
    }

    boolean this_present_maxParts = true && this.isSetMaxParts();
    boolean that_present_maxParts = true && that.isSetMaxParts();
    if (this_present_maxParts || that_present_maxParts) {
      if (!(this_present_maxParts && that_present_maxParts))
        return false;
      if (this.maxParts != that.maxParts)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_dbName = true && (isSetDbName());
    builder.append(present_dbName);
    if (present_dbName)
      builder.append(dbName);

    boolean present_tblName = true && (isSetTblName());
    builder.append(present_tblName);
    if (present_tblName)
      builder.append(tblName);

    boolean present_expr = true && (isSetExpr());
    builder.append(present_expr);
    if (present_expr)
      builder.append(expr);

    boolean present_defaultPartitionName = true && (isSetDefaultPartitionName());
    builder.append(present_defaultPartitionName);
    if (present_defaultPartitionName)
      builder.append(defaultPartitionName);

    boolean present_maxParts = true && (isSetMaxParts());
    builder.append(present_maxParts);
    if (present_maxParts)
      builder.append(maxParts);

    return builder.toHashCode();
  }

  public int compareTo(PartitionsByExprRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    PartitionsByExprRequest typedOther = (PartitionsByExprRequest)other;

    lastComparison = Boolean.valueOf(isSetDbName()).compareTo(typedOther.isSetDbName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDbName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dbName, typedOther.dbName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTblName()).compareTo(typedOther.isSetTblName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTblName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tblName, typedOther.tblName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetExpr()).compareTo(typedOther.isSetExpr());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetExpr()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.expr, typedOther.expr);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDefaultPartitionName()).compareTo(typedOther.isSetDefaultPartitionName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDefaultPartitionName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.defaultPartitionName, typedOther.defaultPartitionName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMaxParts()).compareTo(typedOther.isSetMaxParts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMaxParts()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.maxParts, typedOther.maxParts);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PartitionsByExprRequest(");
    boolean first = true;

    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("tblName:");
    if (this.tblName == null) {
      sb.append("null");
    } else {
      sb.append(this.tblName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("expr:");
    if (this.expr == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.expr, sb);
    }
    first = false;
    if (isSetDefaultPartitionName()) {
      if (!first) sb.append(", ");
      sb.append("defaultPartitionName:");
      if (this.defaultPartitionName == null) {
        sb.append("null");
      } else {
        sb.append(this.defaultPartitionName);
      }
      first = false;
    }
    if (isSetMaxParts()) {
      if (!first) sb.append(", ");
      sb.append("maxParts:");
      sb.append(this.maxParts);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetDbName()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'dbName' is unset! Struct:" + toString());
    }

    if (!isSetTblName()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'tblName' is unset! Struct:" + toString());
    }

    if (!isSetExpr()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'expr' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class PartitionsByExprRequestStandardSchemeFactory implements SchemeFactory {
    public PartitionsByExprRequestStandardScheme getScheme() {
      return new PartitionsByExprRequestStandardScheme();
    }
  }

  private static class PartitionsByExprRequestStandardScheme extends StandardScheme<PartitionsByExprRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, PartitionsByExprRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // DB_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.dbName = iprot.readString();
              struct.setDbNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TBL_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.tblName = iprot.readString();
              struct.setTblNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // EXPR
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.expr = iprot.readBinary();
              struct.setExprIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // DEFAULT_PARTITION_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.defaultPartitionName = iprot.readString();
              struct.setDefaultPartitionNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // MAX_PARTS
            if (schemeField.type == org.apache.thrift.protocol.TType.I16) {
              struct.maxParts = iprot.readI16();
              struct.setMaxPartsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, PartitionsByExprRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.dbName != null) {
        oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
        oprot.writeString(struct.dbName);
        oprot.writeFieldEnd();
      }
      if (struct.tblName != null) {
        oprot.writeFieldBegin(TBL_NAME_FIELD_DESC);
        oprot.writeString(struct.tblName);
        oprot.writeFieldEnd();
      }
      if (struct.expr != null) {
        oprot.writeFieldBegin(EXPR_FIELD_DESC);
        oprot.writeBinary(struct.expr);
        oprot.writeFieldEnd();
      }
      if (struct.defaultPartitionName != null) {
        if (struct.isSetDefaultPartitionName()) {
          oprot.writeFieldBegin(DEFAULT_PARTITION_NAME_FIELD_DESC);
          oprot.writeString(struct.defaultPartitionName);
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetMaxParts()) {
        oprot.writeFieldBegin(MAX_PARTS_FIELD_DESC);
        oprot.writeI16(struct.maxParts);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class PartitionsByExprRequestTupleSchemeFactory implements SchemeFactory {
    public PartitionsByExprRequestTupleScheme getScheme() {
      return new PartitionsByExprRequestTupleScheme();
    }
  }

  private static class PartitionsByExprRequestTupleScheme extends TupleScheme<PartitionsByExprRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, PartitionsByExprRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.dbName);
      oprot.writeString(struct.tblName);
      oprot.writeBinary(struct.expr);
      BitSet optionals = new BitSet();
      if (struct.isSetDefaultPartitionName()) {
        optionals.set(0);
      }
      if (struct.isSetMaxParts()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetDefaultPartitionName()) {
        oprot.writeString(struct.defaultPartitionName);
      }
      if (struct.isSetMaxParts()) {
        oprot.writeI16(struct.maxParts);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, PartitionsByExprRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.dbName = iprot.readString();
      struct.setDbNameIsSet(true);
      struct.tblName = iprot.readString();
      struct.setTblNameIsSet(true);
      struct.expr = iprot.readBinary();
      struct.setExprIsSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.defaultPartitionName = iprot.readString();
        struct.setDefaultPartitionNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.maxParts = iprot.readI16();
        struct.setMaxPartsIsSet(true);
      }
    }
  }

}

