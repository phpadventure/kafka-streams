FROM apache/kafka:4.3.1

# Перемикаємося на root, щоб без проблем скопіювати скрипт та дати права
USER root

COPY entrypoint-kafka.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Повертаємо безпечного дефолтного користувача образу Apache Kafka
USER appuser

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
