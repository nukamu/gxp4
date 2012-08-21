CREATE TABLE workflow_env (
  cwd TEXT,
  mnt_dir TEXT
);

CREATE TABLE ap_log (
  job_id INT,
  cmd TEXT,
  pid INT,
  file_path TEXT,
  created INT,
  read_data TEXT,
  write_data TEXT
);