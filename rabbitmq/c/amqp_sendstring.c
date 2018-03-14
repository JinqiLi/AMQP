
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>

#include "utils.h"

int main(int argc, char const *const *argv) {
  char const *hostname;
  int port, status;
  char const *exchange;
  char const *routingkey;
  char const *messagebody;
  amqp_socket_t *socket = NULL;
  amqp_connection_state_t conn;

  if (argc < 6) {
    fprintf(
        stderr,
        "Usage: amqp_sendstring host port exchange routingkey messagebody\n");
    return 1;
  }

  hostname = argv[1];
  port = atoi(argv[2]);
  exchange = argv[3];
  routingkey = argv[4];
  messagebody = argv[5];

  conn = amqp_new_connection();

  socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    die("creating TCP socket");
  }

  status = amqp_socket_open(socket, hostname, port);
  if (status) {
    die("opening TCP socket");
  }

  die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                               "guest", "guest"),
                    "Logging in");
  amqp_channel_open(conn, 1);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");

  {
    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2; /* persistent delivery mode */

    /**
     * 发布一条消息到broker
     *
     * 使用路由密钥在exchange上发布消息。
     *
     * 请注意，在AMQ协议级别basic.publish是一个异步方法：
     * 这意味着broker发生的错误情况（例如发布到不存在的exchange）将不会反映在此函数的返回值中。
     *
     * \param [in] state 连接对象
     * \param [in] channel 通道标识符
     * \param [in] exchange broker需要发布到的exchange
     * \param [in] routing_key 发布消息使用的路由秘钥
     * \param [in] mandatory 向broker表明该消息必须路由到一个队列。如果broker不能这样做，
     *             它应该用一个basic.return方法来回应。
     * \param [in] immediate 向broker表明该消息必须立即传递给消费者，如果broker不能这样做，
     *             它应该用一个basic.return方法来回应。
     * \param [in] properties 与消息相关的属性
     * \param [in] body 消息体
     * \return 成功返回AMQP_STATUS_OK，失败返回amqp_status_enum值
     *         注意:请注意，basic.publish是一个异步方法，此函数的返回值仅指示消息数据已
     *         成功传输到代理.它并不表示broker发生的故障，例如发布到不存在的exchange.
     *         可能的错误值：
     *         - AMQP_STATUS_TIMER_FAILURE:系统计时器设施返回错误，消息未被发送。
     *         - AMQP_STATUS_HEARTBEAT_TIMEOUT: 等待broker的心跳连接超时，消息未被发送
     *         - AMQP_STATUS_NO_MEMORY:分配内存失败，消息未被发送
     *         - AMQP_STATUS_TABLE_TOO_BIG:属性中的table太大而不适合单个框架，消息未被发送
     *         - AMQP_STATUS_CONNECTION_CLOSED:连接被关闭。
     *         - AMQP_STATUS_SSL_ERROR:发生SSL错误。
     *         - AMQP_STATUS_TCP_ERROR:发生TCP错误，errno或WSAGetLastError()可能提供更多的信息
     * 
     */
    die_on_error(amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange),
                                    amqp_cstring_bytes(routingkey), 0, 0,
                                    &props, amqp_cstring_bytes(messagebody)),
                 "Publishing");
  }

  die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS),
                    "Closing channel");
  die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
                    "Closing connection");
  die_on_error(amqp_destroy_connection(conn), "Ending connection");
  return 0;
}
