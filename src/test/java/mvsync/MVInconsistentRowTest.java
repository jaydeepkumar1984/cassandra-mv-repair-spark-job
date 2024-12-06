package mvsync;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MVInconsistentRowTest {

  @Test
  public void basicTest() {
    RecordPrimaryKey k1 = new RecordPrimaryKey();
    k1.columnNames.add("State");
    k1.columnNames.add("Year");
    k1.columnNames.add("Driver");
    k1.columnTypes.add("String");
    k1.columnTypes.add("String");
    k1.columnTypes.add("String");
    k1.keys.add("NY");
    k1.keys.add("2021");
    k1.keys.add("Driver1");

    assertEquals(
        "Problem: INCONSISTENT\n"
            + "RowKey: State:String:NY,Year:String:2021,Driver:String:Driver1\n"
            + "MainTableEntry: null\n"
            + "MVTableEntry: null",
        new MVInconsistentRow(MVConsistencyState.INCONSISTENT, k1, null, null, null, null)
            .toString());
    assertEquals(
        "Problem: MISSING_IN_BASE_TABLE\n"
            + "RowKey: State:String:NY,Year:String:2021,Driver:String:Driver1\n"
            + "MainTableEntry: null\n"
            + "MVTableEntry: null",
        new MVInconsistentRow(
                MVConsistencyState.MISSING_IN_BASE_TABLE, k1, null, null, null, null)
            .toString());
    assertEquals(
        "Problem: MISSING_IN_MV_TABLE\n"
            + "RowKey: State:String:NY,Year:String:2021,Driver:String:Driver1\n"
            + "MainTableEntry: null\n"
            + "MVTableEntry: null",
        new MVInconsistentRow(
                MVConsistencyState.MISSING_IN_MV_TABLE, k1, null, null, null, null)
            .toString());
  }
}
