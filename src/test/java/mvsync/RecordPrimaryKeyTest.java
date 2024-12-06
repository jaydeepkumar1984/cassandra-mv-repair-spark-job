package mvsync;

import org.junit.Test;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class RecordPrimaryKeyTest {

  @Test
  public void basicTest() {
    RecordPrimaryKey rpk1 = new RecordPrimaryKey();
    rpk1.columnNames.add("State");
    rpk1.columnNames.add("Year");
    rpk1.columnNames.add("Driver");
    rpk1.columnTypes.add("String");
    rpk1.columnTypes.add("String");
    rpk1.columnTypes.add("String");
    rpk1.keys.add("NY");
    rpk1.keys.add("2021");
    rpk1.keys.add("Driver1");

    RecordPrimaryKey rpk2 = new RecordPrimaryKey();
    rpk2.columnNames.add("State");
    rpk2.columnNames.add("Year");
    rpk2.columnNames.add("Driver");
    rpk2.columnTypes.add("String");
    rpk2.columnTypes.add("String");
    rpk2.columnTypes.add("String");
    rpk2.keys.add("SF");
    rpk2.keys.add("2022");
    rpk2.keys.add("Driver2");

    assertTrue(rpk1.equals(rpk1));
    assertFalse(rpk1.equals(rpk2));
    assertEquals("State:String:NY,Year:String:2021,Driver:String:Driver1", rpk1.toString());
    assertEquals("State:String:SF,Year:String:2022,Driver:String:Driver2", rpk2.toString());
  }

  @Test
  public void matches() throws UnknownHostException {
    assertTrue(createRecordPrimaryKey().equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchAscii() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(0, "this is ascii data mismatch");
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchBigInt() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(1, 1234567891L);
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchBlob() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(2, "this is blob data mismatch".getBytes());
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchBoolean() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(3, false);
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchDate() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(4, new java.sql.Date(1234567891L));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchDecimal() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(5, new BigDecimal("1234567890.1234567891"));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchDouble() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(6, 1234567890.1234557890d);
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchFloat() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(7, 1234567890.1234567891);
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchId() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(8, java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440001"));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchInet() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(9, InetAddress.getByName("192.168.1.23"));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchInt() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(10, 1234567891);
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchList() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(11, java.util.Arrays.asList("this is list data mismatch"));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchMap() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(
        12, java.util.Collections.singletonMap("this is map key mismatch", 1234567891));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchSet() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(13, java.util.Collections.singleton("this is set data mismatch"));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchSmallInt() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(14, 12346);
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchText() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(15, "this is text data mismatch");
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchTime() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(16, new java.sql.Time(1234567891L));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchTimestamp() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(17, new java.sql.Timestamp(1234567891L));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchTimeUUID() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(
        18, java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440001"));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchTinyInt() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(19, 124);
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchUUID() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(
        20, java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440001"));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchVarchar() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(21, "this is varchar data mismatch");
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  @Test
  public void mismatchVarint() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = createRecordPrimaryKey();
    recordPrimaryKey.keys.set(22, java.math.BigInteger.valueOf(1234567891));
    assertFalse(recordPrimaryKey.equals(createRecordPrimaryKey()));
  }

  RecordPrimaryKey createRecordPrimaryKey() throws UnknownHostException {
    RecordPrimaryKey recordPrimaryKey = new RecordPrimaryKey();
    recordPrimaryKey.columnNames.add("ascii_col ");
    recordPrimaryKey.columnTypes.add("ascii");
    recordPrimaryKey.keys.add("this is ascii data");

    recordPrimaryKey.columnNames.add("bigint_col ");
    recordPrimaryKey.columnTypes.add("bigint");
    recordPrimaryKey.keys.add(1234567890L);

    recordPrimaryKey.columnNames.add("blob_col ");
    recordPrimaryKey.columnTypes.add("blob");
    recordPrimaryKey.keys.add("this is blob data".getBytes());

    recordPrimaryKey.columnNames.add("boolean_col ");
    recordPrimaryKey.columnTypes.add("boolean");
    recordPrimaryKey.keys.add(true);

    recordPrimaryKey.columnNames.add("date_col ");
    recordPrimaryKey.columnTypes.add("date");
    recordPrimaryKey.keys.add(new java.sql.Date(1234567890L));

    recordPrimaryKey.columnNames.add("decimal_col ");
    recordPrimaryKey.columnTypes.add("decimal");
    recordPrimaryKey.keys.add(new BigDecimal("1234567890.1234567890"));

    recordPrimaryKey.columnNames.add("double_col ");
    recordPrimaryKey.columnTypes.add("double");
    recordPrimaryKey.keys.add(1234567890.1234567890d);

    recordPrimaryKey.columnNames.add("float_col ");
    recordPrimaryKey.columnTypes.add("float");
    recordPrimaryKey.keys.add(1234567890.1234567890f);

    recordPrimaryKey.columnNames.add("id");
    recordPrimaryKey.columnTypes.add("uuid");
    recordPrimaryKey.keys.add(java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));

    recordPrimaryKey.columnNames.add("inet_col ");
    recordPrimaryKey.columnTypes.add("inet");
    recordPrimaryKey.keys.add(InetAddress.getByName("192.168.10.1"));

    recordPrimaryKey.columnNames.add("int_col ");
    recordPrimaryKey.columnTypes.add("int");
    recordPrimaryKey.keys.add(1234567890);

    recordPrimaryKey.columnNames.add("list_col ");
    recordPrimaryKey.columnTypes.add("list<text>");
    recordPrimaryKey.keys.add(java.util.Arrays.asList("this is list data"));

    recordPrimaryKey.columnNames.add("map_col ");
    recordPrimaryKey.columnTypes.add("map<text, int>");
    recordPrimaryKey.keys.add(java.util.Collections.singletonMap("this is map key", 1234567890));

    recordPrimaryKey.columnNames.add("set_col ");
    recordPrimaryKey.columnTypes.add("set<text>");
    recordPrimaryKey.keys.add(java.util.Collections.singleton("this is set data"));

    recordPrimaryKey.columnNames.add("smallint_col ");
    recordPrimaryKey.columnTypes.add("smallint");
    recordPrimaryKey.keys.add(12345);

    recordPrimaryKey.columnNames.add("text_col ");
    recordPrimaryKey.columnTypes.add("text");
    recordPrimaryKey.keys.add("this is text data");

    recordPrimaryKey.columnNames.add("time_col ");
    recordPrimaryKey.columnTypes.add("time");
    recordPrimaryKey.keys.add(new java.sql.Time(1234567890L));

    recordPrimaryKey.columnNames.add("timestamp_col ");
    recordPrimaryKey.columnTypes.add("timestamp");
    recordPrimaryKey.keys.add(new java.sql.Timestamp(1234567890L));

    recordPrimaryKey.columnNames.add("timeuuid_col ");
    recordPrimaryKey.columnTypes.add("timeuuid");
    recordPrimaryKey.keys.add(java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));

    recordPrimaryKey.columnNames.add("tinyint_col ");
    recordPrimaryKey.columnTypes.add("tinyint");
    recordPrimaryKey.keys.add(123);

    recordPrimaryKey.columnNames.add("uuid_col ");
    recordPrimaryKey.columnTypes.add("uuid");
    recordPrimaryKey.keys.add(java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));

    recordPrimaryKey.columnNames.add("varchar_col ");
    recordPrimaryKey.columnTypes.add("text");
    recordPrimaryKey.keys.add("this is varchar data");

    recordPrimaryKey.columnNames.add("varint_col ");
    recordPrimaryKey.columnTypes.add("varint");
    recordPrimaryKey.keys.add(java.math.BigInteger.valueOf(1234567890));
    return recordPrimaryKey;
  }
}
