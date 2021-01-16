name = "qingstor"

namespace "service" {

  new {
    required = ["credential"]
    optional = ["endpoint", "http_client_options", "pair_policy"]
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
  implement = ["copier", "fetcher", "mover", "multiparter", "reacher", "statistician"]

  new {
    required = ["name"]
    optional = ["disable_uri_cleaning", "http_client_options", "location", "pair_policy", "work_dir"]
  }

  op "delete" {
    optional = ["part_id"]
  }
  op "list" {
    optional = ["list_mode"]
  }
  op "reach" {
    required = ["expire"]
  }
  op "read" {
    optional = ["offset", "read_callback_func", "size"]
  }
  op "write" {
    optional = ["content_md5", "content_type", "offset", "read_callback_func", "storage_class"]
  }
}

pairs {

  pair "disable_uri_cleaning" {
    type = "bool"
  }
}

infos {

  info "object" "meta" "storage-class" {
    type = "string"
  }
}
