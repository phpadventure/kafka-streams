#!/bin/sh

# 1. Запускаємо офіційний скрипт старту Kafka у фоновому режимі
/etc/kafka/docker/run &
KAFKA_PID=$!

echo "Очікування підняття Java-процесу Kafka..."
until /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server 127.0.0.1:9092 > /dev/null 2>&1; do
  sleep 2
done

# 2. Перевіряємо, чи передали змінну з топіками
if [ -n "$KAFKA_BOOTSTRAP_TOPICS" ]; then
  echo "Знайдено черги для створення: $KAFKA_BOOTSTRAP_TOPICS"

  # Розбиваємо рядок за комою та створюємо кожну чергу
  IFS=','
  for topic in $KAFKA_BOOTSTRAP_TOPICS; do
    # Видаляємо випадкові пробіли, якщо вони є
    topic=$(echo "$topic" | xargs)

    echo "Створення черги: $topic..."
    /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
      --bootstrap-server 127.0.0.1:9092 \
      --topic "$topic" \
      --partitions 3 \
      --replication-factor 1
  done
  unset IFS
  echo "Усі черги успішно ініціалізовано!"
else
  echo "Змінна KAFKA_BOOTSTRAP_TOPICS порожня. Додаткові черги не створюються."
fi

# 3. Повертаємо процес Kafka на передній план
wait $KAFKA_PID
