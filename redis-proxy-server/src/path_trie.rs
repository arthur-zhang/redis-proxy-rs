use std::collections::HashMap;

use regex::Regex;

pub struct PathTrie {
    root: Node,
    cached_sep_regex: Regex,
}

impl PathTrie {
    pub fn new(list: Vec<String>, sep_regex_str: &str) -> anyhow::Result<Self> {
        let sep_regex = Regex::new(sep_regex_str)?;
        let root = Node::default();
        let mut trie = PathTrie { root, cached_sep_regex: sep_regex };

        for it in list {
            let parts = trie.get_seg_parts(&it);
            trie.insert(&parts);
        }
        Ok(trie)
    }
    pub fn insert(&mut self, segs: &[&str]) {
        if segs.is_empty() {
            return;
        }

        self.root.insert(segs);
    }
    pub fn exists_path(&self, s: &str) -> bool {
        let seg_parts = self.get_seg_parts(s);
        self.root.exists(&seg_parts, 0)
    }

    #[inline]
    fn get_seg_parts<'a>(&self, path: &'a str) -> Vec<&'a str> {
        self.cached_sep_regex.split(path).filter(|it| !it.is_empty()).collect::<Vec<_>>()
    }
    pub fn dump(&self) {
        self.root.dump(0);
    }
}


#[derive(Default, Debug)]
pub struct Node {
    name: String,
    children: HashMap<String, Node>,
}

impl Node {
    pub fn new(name: &str) -> Self {
        Node {
            name: name.to_string(),
            children: HashMap::new(),
        }
    }

    pub fn insert(&mut self, parts: &[&str]) {
        if self.name.as_str() == "**" {
            return;
        }
        if parts.is_empty() {
            return;
        }
        let part = *parts.first().unwrap();

        if !self.children.contains_key(part) {
            let new_node = Node::new(part);
            self.children.insert(part.to_string(), new_node);
        }

        let mut matched_child = self.children.get_mut(part).expect("should not happen");
        if !parts.is_empty() {
            matched_child.insert(&parts[1..]);
        }
    }
    fn exists(&self, parts: &[&str], level: usize) -> bool {
        if level >= parts.len() {
            return self.children.is_empty();
        }
        if self.children.is_empty() {
            return false;
        }
        let part = parts[level];
        let n = self.children.get(part).or_else(|| self.children.get("*"));
        return match n {
            None => {
                self.children.contains_key("**")
            }
            Some(n) => {
                n.exists(parts, level + 1)
            }
        };
    }
    pub fn dump(&self, level: usize) {
        if level == 0 {
            println!(".")
        } else {
            let mut indent = String::new();
            for _ in 0..level {
                indent.push_str("\t");
            }
            indent.push_str("└──");
            indent.push_str(&self.name);
            println!("{}", indent)
        }
        for (_, child) in self.children.iter() {
            child.dump(level + 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use crate::path_trie::PathTrie;

    #[test]
    fn test_new() {
        let text = "some:text,with.different:separators,here";
        let separators = Regex::new(r"[:,.]").unwrap();
        let tokens: Vec<&str> = separators.split(text).collect();
        println!("{:?}", tokens);
    }

    #[test]
    fn test_dump() {
        let list = vec!["/account/login",
                        "/manage/faq/*",
                        "/v2/subCourses/share/*/comments",
                        "/api/v1/lms/**",
                        "seewo:easicare:userinfo:*",
        ]
            .into_iter().map(|it| it.to_string()).collect::<Vec<_>>();
        let trie = PathTrie::new(list, "[/:]").unwrap();

        trie.dump();

        assert!(trie.exists_path("/account/login"));
        assert!(trie.exists_path("/api/v1/lms/123"));
        assert!(!trie.exists_path("/api/v1"));
        assert!(trie.exists_path("/v2/subCourses/share/111/comments"));
        assert!(!trie.exists_path("/v2/subCourses/share/111/111/comments"));
        assert!(trie.exists_path("seewo:easicare:userinfo:123"));
    }
}