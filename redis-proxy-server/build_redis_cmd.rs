use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use serde_json::Error;

use redis_proxy_common::command::RedisCmdDescribeEntity;

fn main() {
    println!("cargo:rerun-if-changed=../redis-command");

    let command_holder_rs_path = "../redis-proxy-common/src/command/holder.rs";
    let cmd_json_path = "../redis-command";

    let mut all_cmd_map = BTreeMap::new();
    let mut all_multi_cmd_map = BTreeMap::new();

    fs::read_dir(cmd_json_path).unwrap().for_each(|entry| {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() && path.extension().unwrap() == "json" {
            let cmd = fs::read_to_string(path).unwrap();
            let cmd_map: Result<HashMap<String, RedisCmdDescribeEntity>, Error> = serde_json::from_str(&cmd);
            let cmd_map = cmd_map.expect("cmd_map should be deserialized");
            cmd_map.iter().for_each(|(k, v)| {
                let mut key = k.clone();
                if let Some(container) = v.container.as_ref() {
                    key = format!("{} {}", container, k);
                    all_multi_cmd_map.insert(container.clone(), true);
                }
                all_cmd_map.insert(key, v.clone());
            });
            //generate_file(format!("../{}", "temp.json"), cmd.as_bytes());
        }
    });

    let mut multi_cmd_map = String::new();
    for (container, _) in all_multi_cmd_map {
        multi_cmd_map.push_str(&format!("        (SmolStr::from(\"{}\"), true),\n", container));
    }

    let mut cmd_map = String::new();
    for (k, v) in all_cmd_map {
        let head = format!("        (SmolStr::from(\"{}\"), RedisCmdDescribeEntity {{\n", k);
        let summary = format!("            summary: String::from(\"{}\"),\n", v.summary);
        let container = if let Some(c) = v.container {
            format!("            container: Some(String::from(\"{}\")),\n", c)
        } else {
            String::from("            container: None,\n")
        };
        let group = format!("            group: String::from(\"{}\"),\n", v.group);
        let since = format!("            since: String::from(\"{}\"),\n", v.since);
        let arity = format!("            arity: {},\n", v.arity);
        let function = if let Some(f) = v.function {
            format!("            function: Some(String::from(\"{}\")),\n", f)
        } else {
            String::from("            function: None,\n")
        };
        let command_flags = if let Some(f) = v.command_flags {
            let mut cf_vec = String::from("            command_flags: Some(vec![");
            for flag in f {
                cf_vec.push_str(&format!("String::from(\"{}\"),", flag));
            }
            cf_vec.push_str("]),\n");
            cf_vec
        } else {
            String::from("            command_flags: None,\n")
        };
        let key_specs = if let Some(key_specs) = v.key_specs {
            let mut ks_vec = String::from("            key_specs: Some(vec![");
            for ks in key_specs {
                ks_vec.push_str("                KeySpecs{\n");
                ks_vec.push_str("                    flags: vec![");
                for flag in ks.flags {
                    ks_vec.push_str(&format!("String::from(\"{}\"),", flag));
                }
                ks_vec.push_str("],\n");
                ks_vec.push_str("                    begin_search: BeginSearch{index: ");
                if let Some(index) = ks.begin_search.index {
                    ks_vec.push_str(&format!("Some(Index{{pos: {}}}),", index.pos));
                } else {
                    ks_vec.push_str("None,");
                }
                ks_vec.push_str(" keyword: ");
                if let Some(keyword) = ks.begin_search.keyword {
                    ks_vec.push_str(&format!("Some(Keyword{{keyword: String::from(\"{}\"), startfrom: {}}}),", keyword.keyword, keyword.startfrom));
                } else {
                    ks_vec.push_str("None,");
                }
                ks_vec.push_str("},\n");
                ks_vec.push_str("                    find_keys: FindKeys{range: ");
                if let Some(range) = ks.find_keys.range {
                    ks_vec.push_str(&format!("Some(Range{{lastkey: {}, step: {}, limit: {}}}),", range.lastkey, range.step, range.limit));
                } else {
                    ks_vec.push_str("None,");
                }
                ks_vec.push_str(" keynum: ");
                if let Some(keynum) = ks.find_keys.keynum {
                    ks_vec.push_str(&format!("Some(KeyNum{{keynumidx: {}, firstkey: {}, step: {}}}),", keynum.keynumidx, keynum.firstkey, keynum.step));
                } else {
                    ks_vec.push_str("None,");
                }
                ks_vec.push_str("},\n");
                ks_vec.push_str("                },\n");
            }
            ks_vec.push_str("            ]),\n");
            ks_vec
        } else {
            String::from("            key_specs: None,\n")
        };
        let tail = String::from("        }),\n");

        cmd_map.push_str(&head);
        cmd_map.push_str(&summary);
        cmd_map.push_str(&container);
        cmd_map.push_str(&group);
        cmd_map.push_str(&since);
        cmd_map.push_str(&arity);
        cmd_map.push_str(&function);
        cmd_map.push_str(&command_flags);
        cmd_map.push_str(&key_specs);
        cmd_map.push_str(&tail);
    }

    let mut cmd_holder_rs_content = String::new();
    cmd_holder_rs_content.push_str("/**\n");
    cmd_holder_rs_content.push_str(" * warning: This file was generated by build_redis_cmd.rs\n");
    cmd_holder_rs_content.push_str(" * do not modify it manually!\n");
    cmd_holder_rs_content.push_str(" */\n");
    cmd_holder_rs_content.push_str("use std::collections::HashMap;\n\n");
    cmd_holder_rs_content.push_str("use lazy_static::lazy_static;\n");
    cmd_holder_rs_content.push_str("use smol_str::SmolStr;\n\n");
    cmd_holder_rs_content.push_str("use crate::command::{BeginSearch, FindKeys, Index, KeyNum, KeySpecs, Keyword, Range, RedisCmdDescribeEntity};\n\n");
    cmd_holder_rs_content.push_str("lazy_static! {\n");
    cmd_holder_rs_content.push_str("    /**\n");
    cmd_holder_rs_content.push_str("     * Redis command's name(or container) and whether it is a multipart command\n");
    cmd_holder_rs_content.push_str("     */\n");
    cmd_holder_rs_content.push_str("    pub static ref MULTIPART_COMMANDS: HashMap<SmolStr, bool> = HashMap::from([\n");
    cmd_holder_rs_content.push_str(&multi_cmd_map);
    cmd_holder_rs_content.push_str("    ]);\n\n");
    cmd_holder_rs_content.push_str("    /**\n");
    cmd_holder_rs_content.push_str("     * Redis command's full name and its description\n");
    cmd_holder_rs_content.push_str("     */\n");
    cmd_holder_rs_content.push_str("    pub static ref COMMANDS_INFO: HashMap<SmolStr, RedisCmdDescribeEntity> = HashMap::from([\n");
    cmd_holder_rs_content.push_str(&cmd_map);
    cmd_holder_rs_content.push_str("    ]);\n");
    cmd_holder_rs_content.push_str("}\n");

    println!("command holder rs content: {}", cmd_holder_rs_content);

    generate_file(command_holder_rs_path, cmd_holder_rs_content.as_bytes());
}

fn generate_file<P: AsRef<Path>>(path: P, text: &[u8]) {
    let mut f = File::create(path).unwrap();
    f.write_all(text).unwrap()
}