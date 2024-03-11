// use std::sync::Arc;

// use anyhow::bail;
// use redis_proxy::config::Config;

// use crate::blacklist_filter::BlackListFilter;
// use crate::log_filter::LogFilter;
// use crate::mirror_filter::MirrorFilter;
// use crate::traits::Filter;

// fn get_filters(config: Arc<Config>) -> anyhow::Result<Vec<Box<dyn Filter>>> {
//     let mut filters: Vec<Box<dyn Filter>> = vec![];
//     let mut filter_chain_conf = config.filter_chain.clone();
//
//     for filter_name in &config.filter_chain.filters {
//         let filter: Box<dyn Filter> = match filter_name.as_str() {
//             "blacklist" => {
//                 match filter_chain_conf.blacklist.take() {
//                     None => {
//                         bail!("blacklist filter config is required")
//                     }
//                     Some(blacklist) => {
//                         Box::new(BlackListFilter::new(blacklist.block_patterns, &blacklist.split_regex)?)
//                     }
//                 }
//             }
//             "log" => {
//                 Box::new(LogFilter::new())
//             }
//             "mirror" => {
//                 match filter_chain_conf.mirror.take() {
//                     None => {
//                         bail!("mirror filter config is required")
//                     }
//                     Some(mirror) => {
//                         Box::new(MirrorFilter::new(mirror.address.as_str(), &mirror.mirror_patterns, mirror.split_regex.as_str(), mirror.queue_size)?)
//                     }
//                 }
//             }
//             _ => {
//                 bail!("unknown filter: {}", filter_name)
//             }
//         };
//         filters.push(filter);
//     }
//
//     Ok(filters)
// }