CREATE TABLE workflow_env (
  cwd TEXT,
  mnt_dir TEXT
);

CREATE TABLE ap_log (
  cmd TEXT,
  pid INT,
  file_path TEXT,
  created INT,
  read_data TEXT,
  write_data TEXT
);