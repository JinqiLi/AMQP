#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>

#include <assert.h>

#include "utils.h"

int main(int argc, char const *const *argv) {
  char const *hostname;
  int port, status;
  char const *exchange;
  char const *bindingkey;
  amqp_socket_t *socket = NULL;
  amqp_connection_state_t conn;

  amqp_bytes_t queuename;

  if (argc < 5) {
    fprintf(stderr, "Usage: amqp_listen host port exchange bindingkey\n");
    return 1;
  }

  hostname = argv[1];
  port = atoi(argv[2]);
  exchange = argv[3];
  bindingkey = argv[4];

  /*分配并初始化一个新的amqp_connection_state_t对象，用该函数创建的
  amqp_connection_state_t对象需要用amqp_destroy_connection()函数来释放*/
  conn = amqp_new_connection();

  /*创建一个新的TCP socket
  需调用amqp_connection_close()释放socket资源*/
  socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    die("creating TCP socket");
  }


/**
 * 打开socket连接
 *
 * 该函数打开从amqp_tcp_socket_new()或amqp_ssl_socket_new()返回的socket连接。
 * 该函数应当在设置socket选择之后并在使用amqp_set_socket（）分配socket到AMQP连接
 * 之前调用。
 *
 * 成功返回AMQP_STATUS_OK，失败返回一个amqp_status_enum
 */
  status = amqp_socket_open(socket, hostname, port);
  if (status) {
    die("opening TCP socket");
  }

  /**
 *login到broker
 *
 * 使用amqp_open_socket和amqp_set_sockfd后，调用amqp_login完成到broker的连接
 *
 * \param [in] state 连接对象
 * \param [in] vhost 虚拟主机连接到broker，大多数broker默认为“/”
 * \param [in] channel_max 连接通道数量的限制，0代表无限制，较好的默认值是AMQP_DEFAULT_MAX_CHANNELS
 * \param [in] frame_max 线路上的AMQP帧的最大大小以请求代理进行此连接,最小4096，
 * 最大2^31-1，较好的默认值是131072 (128KB)或者AMQP_DEFAULT_FRAME_SIZE
 * \param [in] heartbeat 心跳帧到broker的请求之间的秒数。设0表示禁用心跳
 * \param [in] sasl_method SASL method用来验证broker，以下是SASL methods的实现:
 *             -AMQP_SASL_METHOD_PLAIN：该方法需要按如下顺序跟两个参数：
 *               const char* username, and const char* password.
 *             -AMQP_SASL_METHOD_EXTERNAL：该方法需要跟参数：
 *               const char* identity.
 *
 * 返回值：amqp_rpc_reply_t 标明成功或失败
 */
  die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                               "guest", "guest"),
                    "Logging in");


  amqp_channel_open(conn, 1);

  /**
 * 获取最后一个全局amqp_rpc_reply
 *
 * 此API方法对应于大多数同步的AMQP方法返回一个指向解码方法结果的指针。
 *
 * \param [in] state 连接对象
 * \return 最新的amqp_rpc_reply_t
 * - r.reply_type == AMQP_RESPONSE_NORMAL. RPC已成功完成
 * - r.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION. broker返回异常
 * - r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION. 库内发生异常
 */
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");

  {
    amqp_queue_declare_ok_t *r = amqp_queue_declare(
        conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");

/**
 * 复制一个amqp_bytes_t缓冲区。
 *
 * clone缓冲区并复制内容
 *
 * 与输出相关的内存分配给amqp_bytes_malloc()，且应该amqp_bytes_free()释放
 */
    queuename = amqp_bytes_malloc_dup(r->queue);
    if (queuename.bytes == NULL) {
      fprintf(stderr, "Out of memory while copying queue name");
      return 1;
    }
  }

  amqp_queue_bind(conn, 1, queuename, amqp_cstring_bytes(exchange),
                  amqp_cstring_bytes(bindingkey), amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");

  amqp_basic_consume(conn, 1, queuename, amqp_empty_bytes, 0, 1, 0,
                     amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");

  {
    for (;;) {
      amqp_rpc_reply_t res;
      amqp_envelope_t envelope;

      /**
      * 释放amqp_connection_state_t占用的内存
      *
      * 释放与任何通道相关的amqp_connection_state_t对象拥有的内存，允许库重用。
      * 在调用该函数之前使用库返回的任何内存，会导致未定义的行为。
      */
      amqp_maybe_release_buffers(conn);

      /**
      * 等待并消费一条消息
      *
      * 在任何频道上等待basic.deliver方法，一旦收到basic.deliver它读取该消息，并返回。
      * 如果在basic.deliver之前接收到任何其他方法，则此函数将返回一个包含
      * ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION和
      * ret.library_error == AMQP_STATUS_UNEXPECTED_STATE的amqp_rpc_reply_t。
      * 然后调用者应该调用amqp_simple_wait_frame()来读取这个帧并采取适当的行动。
      *
      * 在使用amqp_basic_consume()函数启动消费者之后，应该使用此函数
      *
      *  \returns 一个amqp_rpc_reply_t对象，成功时，ret.reply_type == AMQP_RESPONSE_NORMAL
      *  如果ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION，并且
      *  ret.library_error == AMQP_STATUS_UNEXPECTED_STATE
      *  如果收到AMQP_BASIC_DELIVER_METHOD以外的帧，则调用者应调用amqp_simple_wait_frame()
      *  来读取此帧并采取适当的操作。
      */
      res = amqp_consume_message(conn, &envelope, NULL, 0);

      if (AMQP_RESPONSE_NORMAL != res.reply_type) {
        break;
      }

      printf("Delivery %u, exchange %.*s routingkey %.*s\n",
             (unsigned)envelope.delivery_tag, (int)envelope.exchange.len,
             (char *)envelope.exchange.bytes, (int)envelope.routing_key.len,
             (char *)envelope.routing_key.bytes);

      if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
        printf("Content-type: %.*s\n",
               (int)envelope.message.properties.content_type.len,
               (char *)envelope.message.properties.content_type.bytes);
      }
      printf("----\n");

      amqp_dump(envelope.message.body.bytes, envelope.message.body.len);

      /**
       * 释放在amqp_consume_message()中分配的与amqp_envelope_t相关联的内存
       */
      amqp_destroy_envelope(&envelope);
    }
  }

  /**
 * 关闭 channel
 *
 * \param [in] state 连接对象
 * \param [in] channel channel标识符
 * \param [in] 关闭channel的原因，最好默认为AMQP_REPLY_SUCCESS
 * \return amqp_rpc_reply_t 表示成功或失败
 */
  die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS),
                    "Closing channel");

  /**
   * 关闭整个连接
   *
   * 隐式关闭所有连接，通知broker连接正在关闭，收到来自broker的确认后，
   * 关闭socket
   *
 * \param [in] state 连接对象
 * \param [in] 关闭channel的原因，最好默认为AMQP_REPLY_SUCCESS
 * \return amqp_rpc_reply_t 表示成功或失败
   */
  die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
                    "Closing connection");

  /**
 * 销毁amqp_connection_state_t对象
 *
 * 销毁使用amqp_new_connection()创建的amqp_connection_state_t对象
 * 如果与broker的连接处于打开状态，则会以200（成功）的答复代码隐式关闭。
 * 任何会被amqp_maybe_release_buffers()或amqp_maybe_release_buffers_on_channel()
 * 释放的内存都将被释放，并且使用该内存将导致未定义的行为。
 *
 * \return  成功返回AMQP_STATUS_OK. 失败返回amqp_status_enum值 
 */
  die_on_error(amqp_destroy_connection(conn), "Ending connection");

  return 0;
}
