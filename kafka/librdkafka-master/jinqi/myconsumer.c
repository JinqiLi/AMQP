#include <string.h>
#include <stdlib.h>
#include <syslog.h>
#include <signal.h>
#include <error.h>
#include <getopt.h>

#include "../src/rdkafka.h"

static int run = 1;
static rd_kafka_t *rk;
static int exit_eof = 0;
static int wait_eof = 0;  /* number of partitions awaiting EOF */
static int quiet = 0;
static  enum {
  OUTPUT_HEXDUMP,
  OUTPUT_RAW,
} output = OUTPUT_HEXDUMP;

static void stop (int sig) {
  if (!run)
    exit(1);
  run = 0;
  fclose(stdin); /* abort fgets() */
}

static void sig_usr1 (int sig) {
  rd_kafka_dump(stdout, rk);
}

static void logger (const rd_kafka_t *rk, int level,
        const char *fac, const char *buf) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
    (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
    level, fac, rk ? rd_kafka_name(rk) : NULL, buf);
}

static void hexdump (FILE *fp, const char *name, const void *ptr, size_t len) {
  const char *p = (const char *)ptr;
  unsigned int of = 0;


  if (name)
    fprintf(fp, "%s hexdump (%zd bytes):\n", name, len);

  for (of = 0 ; of < len ; of += 16) {
    char hexen[16*3+1];
    char charen[16+1];
    int hof = 0;

    int cof = 0;
    int i;

    for (i = of ; i < (int)of + 16 && i < (int)len ; i++) {
      hof += sprintf(hexen+hof, "%02x ", p[i] & 0xff);
      cof += sprintf(charen+cof, "%c",
               isprint((int)p[i]) ? p[i] : '.');
    }
    fprintf(fp, "%08x: %-48s %-16s\n",
      of, hexen, charen);
  }
}

static void print_partition_list (FILE *fp,
                                  const rd_kafka_topic_partition_list_t
                                  *partitions) {
        int i;
        for (i = 0 ; i < partitions->cnt ; i++) {
                fprintf(stderr, "%s %s [%"PRId32"] offset %"PRId64,
                        i > 0 ? ",":"",
                        partitions->elems[i].topic,
                        partitions->elems[i].partition,
      partitions->elems[i].offset);
        }
        fprintf(stderr, "\n");

}

static void rebalance_cb (rd_kafka_t *rk,
                          rd_kafka_resp_err_t err,
        rd_kafka_topic_partition_list_t *partitions,
                          void *opaque) {

  fprintf(stderr, "%% Consumer group rebalanced: ");

  switch (err)
  {
  case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
    fprintf(stderr, "assigned:\n");
    print_partition_list(stderr, partitions);
    rd_kafka_assign(rk, partitions);
    wait_eof += partitions->cnt;
    break;

  case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
    fprintf(stderr, "revoked:\n");
    print_partition_list(stderr, partitions);
    rd_kafka_assign(rk, NULL);
    wait_eof = 0;
    break;

  default:
    fprintf(stderr, "failed: %s\n",
                        rd_kafka_err2str(err));
                rd_kafka_assign(rk, NULL);
    break;
  }
}

/**
 * Handle and print a consumed message.
 * Internally crafted messages are also used to propagate state from
 * librdkafka to the application. The application needs to check
 * the `rkmessage->err` field for this purpose.
 */
static void msg_consume (rd_kafka_message_t *rkmessage,
       void *opaque) {
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      fprintf(stderr,
        "%% Consumer reached end of %s [%"PRId32"] "
             "message queue at offset %"PRId64"\n",
             rd_kafka_topic_name(rkmessage->rkt),
             rkmessage->partition, rkmessage->offset);

      if (exit_eof && --wait_eof == 0) {
        fprintf(stderr,
                "%% All partition(s) reached EOF: "
                "exiting\n");
        run = 0;
      }

      return;
    }

    if (rkmessage->rkt)
            fprintf(stderr, "%% Consume error for "
                    "topic \"%s\" [%"PRId32"] "
                    "offset %"PRId64": %s\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    rkmessage->partition,
                    rkmessage->offset,
                    rd_kafka_message_errstr(rkmessage));
    else
            fprintf(stderr, "%% Consumer error: %s: %s\n",
                    rd_kafka_err2str(rkmessage->err),
                    rd_kafka_message_errstr(rkmessage));

    if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
        rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
          run = 0;
    return;
  }

  if (!quiet)
    fprintf(stdout, "%% Message (topic %s [%"PRId32"], "
                        "offset %"PRId64", %zd bytes):\n",
                        rd_kafka_topic_name(rkmessage->rkt),
                        rkmessage->partition,
      rkmessage->offset, rkmessage->len);

  if (rkmessage->key_len) {
    if (output == OUTPUT_HEXDUMP)
      hexdump(stdout, "Message Key",
        rkmessage->key, rkmessage->key_len);
    else
      printf("Key: %.*s\n",
             (int)rkmessage->key_len, (char *)rkmessage->key);
  }

  if (output == OUTPUT_HEXDUMP)
    hexdump(stdout, "Message Payload",
      rkmessage->payload, rkmessage->len);
  else
    printf("%.*s\n",
           (int)rkmessage->len, (char *)rkmessage->payload);
}

int main(int argc, char **argv){
  char *brokers = "localhost:9092";
  int partition = RD_KAFKA_PARTITION_UA;
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;
  rd_kafka_topic_partition_list_t *topics;
  rd_kafka_resp_err_t err;
  int is_subscription;
  char tmp[16];
  char errstr[512];
  int i;
  int opt;
  char *group = NULL;

  //kafka configuration
  conf = rd_kafka_conf_new();

  //set logger
  rd_kafka_conf_set_log_cb(conf, logger);

  //quick termination
  snprintf(tmp, sizeof(tmp), "%i", SIGIO);
  rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);

  //topic configuration
  topic_conf = rd_kafka_topic_conf_new();

  //brokers = argv[1];
  //topic = argv[2];

  while ((opt = getopt(argc, argv, "g:b:qd:eX:As:DO")) != -1){
    switch (opt) {
    case 'b':
      brokers = optarg;
      break;
    case 'g':
      group = optarg;
      break;
    default:
      break;
  }
  }

  signal(SIGINT, stop);
  signal(SIGUSR1, sig_usr1);

  /* Consumer groups require a group id */
  if (!group)
          group = "rdkafka_consumer_example";
  if (rd_kafka_conf_set(conf, "group.id", group,
                        errstr, sizeof(errstr)) !=
      RD_KAFKA_CONF_OK) {
          fprintf(stderr, "%% %s\n", errstr);
          exit(1);
  }

  /* Consumer groups always use broker based offset storage */
  if (rd_kafka_topic_conf_set(topic_conf, "offset.store.method",
                              "broker",
                              errstr, sizeof(errstr)) !=
      RD_KAFKA_CONF_OK) {
          fprintf(stderr, "%% %s\n", errstr);
          exit(1);
  }

  /* Set default topic config for pattern-matched topics. */
  rd_kafka_conf_set_default_topic_conf(conf, topic_conf);

  /* Callback called on partition assignment changes */
  rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);



  //create kafka handler
  rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if(!rk){
    fprintf(stderr, "%% Failed to create new consumer:%s\n", errstr);
    exit(1);
  }

  //add brokers
  if (rd_kafka_brokers_add(rk, brokers) == 0){
    fprintf(stderr, "%% No valid brokers specified\n");
    exit(1);
  }

  //redirect rd_kafka_poll() to consumer_poll()
  rd_kafka_poll_set_consumer(rk);

  topics = rd_kafka_topic_partition_list_new(argc - optind);
  is_subscription = 1;
  for(i = optind; i < argc; i++){
      //parse "topic[:part]"
      char *topic = argv[i];
      char *t;
      int32_t partition = -1;

      if ((t = strstr(topic, ":"))) {
        *t = '\0';  
        partition = atoi(t+1);
        is_subscription = 0; /* is assignment */
        wait_eof++;
      }

      rd_kafka_topic_partition_list_add(topics, topic, partition);
  }

  if(is_subscription){
    fprintf(stderr, "%% is_subscribing to %d topics\n", topics->cnt);
    if((err = rd_kafka_subscribe(rk, topics))){
      fprintf(stderr, "%% Failed to start consuming topics: %s\n", rd_kafka_err2str(err));
      exit(1);
    }
  }else{
    fprintf(stderr, "%% Assigning %d partitions\n", topics->cnt);

    if((err = rd_kafka_assign(rk, topics))){
      fprintf(stderr, "%% Failed to assign partitions: %s\n", rd_kafka_err2str(err));
    }
  }

  while(run){
    rd_kafka_message_t *rkmessage;

    rkmessage = rd_kafka_consumer_poll(rk, 1000);
    if(rkmessage){
      msg_consume(rkmessage, NULL);
      rd_kafka_message_destroy(rkmessage);
    }
  }

done:
    err = rd_kafka_consumer_close(rk);
    if(err){
      fprintf(stderr, "%% Failed to close consumer: %s\n", rd_kafka_err2str(err));
    }else{
      fprintf(stderr, "%% Consumer closed\n");
    }

    rd_kafka_topic_partition_list_destroy(topics);

    //destroy handle
    rd_kafka_destroy(rk);
  
  run = 5;
  while(run-- > 0 && rd_kafka_wait_destroyed(1000) == -1){
    printf("Waiting for librdkafka to decommission\n");
  }
  if(run <= 0){
    rd_kafka_dump(stdout, rk);
  }

  return 0;
}