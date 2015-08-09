#include <nan.h>

using namespace v8;

NAN_METHOD(Hasher) {
  unsigned int hash = ((int) info[0]->NumberValue() << 24)
    | ((int) info[1]->NumberValue() << 16)
    | ((int) info[2]->NumberValue() << 8)
    | (int) info[3]->NumberValue();

  info.GetReturnValue().Set(static_cast<double>(hash));
}

void init(Handle<Object> target) {
  target->Set(
      Nan::New<String>("hash").ToLocalChecked()
    , Nan::New<FunctionTemplate>(Hasher)->GetFunction()
  );
}

NODE_MODULE(hashvalue, init)
