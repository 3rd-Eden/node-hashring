{
  "targets": [
    {
      "target_name": "hashvalue",
      'include_dirs': ["<!(node -e \"require('nan')\")"],
      "sources": [ "src/hashvalue.cc" ]
    }
  ]
}
