#include <v8.h> 
#include <node.h>

using namespace v8;

Handle<Value> Hasher(const Arguments& args) {
  HandleScope scope;

  unsigned int hash = ((int) args[0]->NumberValue() << 24)
    | ((int) args[1]->NumberValue() << 16)
    | ((int) args[2]->NumberValue() << 8)
    | (int) args[3]->NumberValue();

  return scope.Close(Number::New(hash));
}

void init(Handle<Object> target) {
  target->Set(
      String::NewSymbol("hash")
    , FunctionTemplate::New(Hasher)->GetFunction()
  );
}

NODE_MODULE(hashvalue, init)
