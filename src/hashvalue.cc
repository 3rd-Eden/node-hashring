#include <nan.h>
using namespace v8;

NAN_METHOD(Hasher) {
  Nan::HandleScope();

  unsigned int hash = ((int) info[0]->NumberValue() << 24)
    | ((int) info[1]->NumberValue() << 16)
    | ((int) info[2]->NumberValue() << 8)
    | (int) info[3]->NumberValue();

  info.GetReturnValue().Set(Nan::New<Number>(hash));
}

NAN_MODULE_INIT(init) {
  target->Set(
      Nan::New<String>("hash").ToLocalChecked()
    , Nan::GetFunction(Nan::New<FunctionTemplate>(Hasher)).ToLocalChecked()
  );
}

NODE_MODULE(hashvalue, init)
