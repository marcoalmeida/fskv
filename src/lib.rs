use md5;
use std::fs;
use std::io::prelude::*;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const DIRECTORY_TREE_HEIGHT: usize = 3;
const SINGLE_DIRECTORY_LENGTH: usize = 4;

#[derive(Clone, Copy, Debug)]
pub struct Store {
    root_directory: &'static str,
}

impl Store {
    fn get_key_path(&self, key: &str) -> PathBuf {
        // create keys in a (hopefully uniformly random) directory
        // structure with N levels
        //
        // the "hopefully uniformly random" part should be ensured by
        // taking chunks of the MD5 sum digest
        let digest = format!("{:x}", md5::compute(key));
        let mut root = PathBuf::from(&self.root_directory);
        for i in 0..DIRECTORY_TREE_HEIGHT {
            root.push(
                digest
                    .chars()
                    .skip(SINGLE_DIRECTORY_LENGTH * i)
                    .take(SINGLE_DIRECTORY_LENGTH)
                    .collect::<String>(),
            );
        }

        return root;
    }

    pub fn new(root_dir: &'static str, create: bool) -> Result<Store, Error> {
        let store = Store {
            root_directory: root_dir,
        };

        if create {
            fs::create_dir_all(root_dir).and(Ok(store))
        } else {
            fs::metadata(root_dir).and(Ok(store))
        }
    }

    pub fn put(&self, key: &str, value: &str) -> Result<(), Error> {
        // create the directory structure
        let key_path = self.get_key_path(&key);
        fs::create_dir_all(&key_path)?;
        // now save the thing using create_new -- it's atomic
        let key_file = Path::new(&key_path).join(&key);
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&key_file)
            .and_then(|mut f| f.write_all(&value.as_bytes()))
    }

    pub fn get(&self, key: &str) -> Result<String, Error> {
        let key_path = self.get_key_path(&key).join(&key);
        let mut value = String::new();

        fs::File::open(&key_path)
            .and_then(|mut f| f.read_to_string(&mut value))
            .map(|_| value)
    }

    pub fn update(&self, key: &str, value: &str) -> Result<(), Error> {
        let key_path = self.get_key_path(&key);
        // do upsert
        match fs::metadata(&key_path) {
            // write to a new, random, file and then move
            Ok(_) => {
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("failed to get system time");
                // even under load, the probability of 2 requests
                // happening at the same nanosecond is low; very low
                let tmp = format!("{}", now.as_nanos());
                let tmp_file = Path::new(&key_path).join(&tmp);
                let key_file = Path::new(&key_path).join(&key);
                // write to the temporary file and then move to the
                // actual key; or exit on error
                fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&tmp_file)
                    .and_then(|mut f| f.write_all(&value.as_bytes()))
                    .and_then(|_| fs::rename(&tmp_file, &key_file))
            }
            // just create a new entry
            Err(_) => self.put(&key, &value),
        }
    }

    pub fn delete(&self, key: &str) -> Result<(), Error> {
        fs::remove_file(&self.get_key_path(&key).join(&key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::vec::Vec;

    fn teardown(directories: Vec<&str>) {
        for d in directories.iter() {
            fs::remove_dir_all(d).unwrap_or_else(|e| print!("{:?}", e));
        }
    }

    #[test]
    fn test_new() {
        // fail to create (assuming we're not running tests as root)
        let ds = Store::new("/foo", true);
        assert_eq!(ds.is_ok(), false);
        // succeed creating a directory in `cwd`
        let ds = Store::new("fskv_test", true);
        assert_eq!(ds.is_ok(), true);
        // succeed trying to create it a second time
        let ds = Store::new("fskv_test", true);
        assert_eq!(ds.is_ok(), true);
        // also succeed when using an existing directory
        let ds = Store::new("fskv_test", false);
        assert_eq!(ds.is_ok(), true);

        teardown(vec!["fskv_test"]);
    }

    #[test]
    fn test_put() {
        let ds = Store::new("fskv_test", true);
        assert_eq!(ds.is_ok(), true);
        let ds = ds.unwrap();
        assert_eq!(ds.put("foo", "bar").is_ok(), true);
        // put is atomic and requires the key to not exist already
        assert_eq!(ds.put("foo", "bar").is_ok(), false);

        teardown(vec!["fskv_test"]);
    }
}
