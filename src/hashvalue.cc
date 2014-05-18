#include <nan.h>

using namespace v8;

NAN_METHOD(Hasher) {
  NanScope();

  unsigned int hash = ((int) args[0]->NumberValue() << 24)
    | ((int) args[1]->NumberValue() << 16)
    | ((int) args[2]->NumberValue() << 8)
    | (int) args[3]->NumberValue();

  NanReturnValue(NanNew<Number>(hash));
}

void init(Handle<Object> target) {
  target->Set(
      NanNew<String>("hash")
    , NanNew<FunctionTemplate>(Hasher)->GetFunction()
  );
}

NODE_MODULE(hashvalue, init)
