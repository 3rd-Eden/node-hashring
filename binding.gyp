{
  "targets": [
    {
      "target_name": "hashvalue",
      'include_dirs': ["<!(node -p -e \"require('path').dirname(require.resolve('nan'))\")"],
      "sources": [ "src/hashvalue.cc" ]
    }
  ]
}
