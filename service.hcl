name = "qingstor"

namespace "service" {

  new {
    required = ["credential"]
    optional = ["additional_user_agent", "endpoint", "http_client_options", "pair_policy"]
  }

  op "create" {
    required = ["location"]
  }
  op "delete" {
    optional = ["location"]
  }
  op "get" {
    optional = ["location"]
  }
  op "list" {
    optional = ["location"]
  }
}
namespace "storage" {
  implement = ["copier", "dir_lister", "index_segmenter", "mover", "prefix_lister", "prefix_segments_lister", "reacher", "segmenter", "statistician"]

  new {
    required = ["name"]
    optional = ["disable_uri_cleaning", "http_client_options", "location", "pair_policy", "work_dir"]
  }

  op "reach" {
    required = ["expire"]
  }
  op "read" {
    optional = ["offset", "read_callback_func", "size"]
  }
  op "write" {
    required = ["size"]
    optional = ["content_md5", "content_type", "offset", "read_callback_func", "storage_class"]
  }
  op "write_index_segment" {
    optional = ["read_callback_func"]
  }
}

pairs {

  pair "additional_user_agent" {
    type = "string"
  }
  pair "disable_uri_cleaning" {
    type = "bool"
  }
}

infos {

  info "object" "meta" "storage-class" {
    type = "string"
  }
}
