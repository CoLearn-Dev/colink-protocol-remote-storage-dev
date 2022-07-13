1.
```
bash install_colink.sh
```
2.
```
bash policy_module.sh
```
3. Start the protocol (get user jwt from user_token.txt)
```
e.g. cargo run -- --addr http://127.0.0.1:8080 --jwt jwt
```
4. Run app code example
```
e.g. cargo run --example app http://127.0.0.1:8080 jwt1 jwt2 hello
```
