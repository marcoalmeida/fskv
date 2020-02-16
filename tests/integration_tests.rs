use fskv::Store;
use std::fs;
use std::vec::Vec;

fn teardown(directories: Vec<&str>) {
    for d in directories.iter() {
        fs::remove_dir_all(d).unwrap_or_else(|e| print!("{:?}", e));
    }
}

#[test]
fn test_get() {
    let ds = Store::new("fskv_test", true);
    assert_eq!(ds.is_ok(), true);
    let ds = ds.unwrap();
    // does not exist, should fail
    assert_eq!(ds.get("getkey_doesnt_exist").is_ok(), false);
    // put something and read it back
    assert_eq!(ds.put("getkey", "foo").is_ok(), true);
    assert_eq!(ds.get("getkey").is_ok(), true);

    teardown(vec!["fskv_test"]);
}

#[test]
fn test_update() {
    let ds = Store::new("fskv_test", true);
    assert_eq!(ds.is_ok(), true);
    let ds = ds.unwrap();
    // update
    assert_eq!(ds.put("update", "yes").is_ok(), true);
    assert_eq!(ds.update("update", "yes").is_ok(), true);
    // insert new via update
    assert_eq!(ds.update("upsert", "yes").is_ok(), true);

    teardown(vec!["fskv_test"]);
}

#[test]
fn test_delete() {
    let ds = Store::new("fskv_test", true);
    assert_eq!(ds.is_ok(), true);
    let ds = ds.unwrap();
    assert_eq!(ds.put("delkey", "foo").is_ok(), true);
    assert_eq!(ds.get("delkey").is_ok(), true);
    assert_eq!(ds.delete("delkey").is_ok(), true);
    assert_eq!(ds.get("delkey").is_ok(), false);
    // does not exist, should fail
    assert_eq!(ds.delete("delkey_does_not_exist").is_ok(), false);

    teardown(vec!["fskv_test"]);
}
