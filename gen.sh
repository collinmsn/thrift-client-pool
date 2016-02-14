
THRIFT_PACKAGE_PREFIX=githup.com/collinmsn/thrift_client_pool/example
THRIFT_IDL=example.thrift
thrift -r --gen go:ignore_initialisms,package_prefix=${THRIFT_PACKAGE_PREFIX} -out . ${THRIFT_IDL}