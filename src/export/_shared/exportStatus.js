const EXPORT_STATUS = {
  ARCHIVED:  3,
  QUEUED:   10,
  RUNNING:  11,
  COMPLETED: 12,
  FAILED:   13,
};

const EXPORT_STATUS_NAME = {
  3:  "Archive",
  10: "Queued",
  11: "Running",
  12: "Completed",
  13: "Failed",
};

module.exports = { EXPORT_STATUS, EXPORT_STATUS_NAME };
