module net.pincette.letterbox.kafka {
  requires net.pincette.common;
  requires java.logging;
  requires java.json;
  requires net.pincette.jes.tel;
  requires net.pincette.netty.http;
  requires net.pincette.json;
  requires net.pincette.rs;
  requires io.netty.buffer;
  requires io.netty.codec.http;
  requires io.opentelemetry.api;
  requires kafka.clients;
  requires net.pincette.rs.json;
  requires net.pincette.rs.kafka;
  requires org.mongodb.driver.core;
  requires net.pincette.jes.util;
  requires net.pincette.jes;
  requires net.pincette.kafka.json;
  requires typesafe.config;
  requires net.pincette.config;

  exports net.pincette.letterbox.kafka;

  opens net.pincette.letterbox.kafka;
}
