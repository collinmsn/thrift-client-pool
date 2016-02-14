
namespace go example

service Example {
i32 add(1:i32 num1, 2:i32 num2),
i32 add_timeout(1:i32 num1, 2:i32 num2, 3:i32 client_timeout_ms),
}
