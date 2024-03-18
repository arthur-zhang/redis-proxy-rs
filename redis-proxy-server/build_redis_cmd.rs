use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=../redis-command");

    let _path = "../redis-proxy-common/src/redis_cmd.rs";

    let cmd_json_path = "../redis-command";
    fs::read_dir(cmd_json_path).unwrap().for_each(|entry| {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() && path.extension().unwrap() == "json" {
            let cmd = fs::read_to_string(path).unwrap();
            generate_file(format!("../{}", "temp.json"), cmd.as_bytes());
        }
    });
}

fn generate_file<P: AsRef<Path>>(path: P, text: &[u8]) {
    let mut f = File::create(path).unwrap();
    f.write_all(text).unwrap()
}