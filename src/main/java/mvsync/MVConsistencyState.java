package mvsync;

public enum MVConsistencyState {
  CONSISTENT,
  MISSING_IN_BASE_TABLE,
  MISSING_IN_MV_TABLE,
  INCONSISTENT,
}
