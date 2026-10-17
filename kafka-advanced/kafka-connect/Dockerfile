FROM confluentinc/cp-kafka-connect:7.6.0

# Install Confluent JDBC Connector plugin
# This adds support for reading from / writing to relational databases (PostgreSQL, MySQL etc.)
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6

# Install PostgreSQL JDBC driver into the connector's lib directory
# The JDBC connector needs this driver to talk to PostgreSQL
RUN curl -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
