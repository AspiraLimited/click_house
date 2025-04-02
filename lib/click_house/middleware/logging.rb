# frozen_string_literal: true

module ClickHouse
  module Middleware
    class Logging < Faraday::Middleware
      # Register under a usable symbol
      Faraday::Response.register_middleware(logging: self)

      SUMMARY_HEADER = 'x-clickhouse-summary'

      attr_reader :logger, :starting, :body

      def initialize(app = nil, logger:)
        @logger = logger
        super(app)
      end

      def call(environment)
        @starting = timestamp
        @body = environment.body if log_body?
        @app.call(environment).on_complete(&method(:on_complete))
      end

      private

      def log_body?
        logger.level == Logger::DEBUG
      end

      def on_complete(env)
        summary = extract_summary(env.response_headers)

        logger.info("\e[1m\e[35mSQL (#{duration_stats_log(env.body)})\e[0m #{query(env)};\e[0m")
        logger.debug(body) if body && !query_in_body?(env)

        logger.info(
          "\e[1m\e[36mRead: #{summary[:read_rows]} rows, #{summary[:read_bytes]}. " \
            "Written: #{summary[:written_rows]} rows, #{summary[:written_bytes]}\e[0m"
        )
      end

      def duration
        timestamp - starting
      end

      def timestamp
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      def query_in_body?(env)
        env.method == :get
      end

      def query(env)
        if query_in_body?(env)
          body
        else
          CGI.parse(env.url.query.to_s).dig('query', 0) || '[NO QUERY]'
        end
      end

      def duration_stats_log(body)
        elapsed = duration
        ch_elapsed = body.dig('statistics', 'elapsed') if body.is_a?(Hash)

        parts = ["Total: #{Util::Pretty.measure(elapsed * 1000)}"]
        parts << "CH: #{Util::Pretty.measure(ch_elapsed * 1000)}" if ch_elapsed
        parts.join(', ')
      end

      def extract_summary(headers)
        raw = headers.fetch(SUMMARY_HEADER, '{}')
        json = JSON.parse(raw)

        {
          read_rows: json['read_rows'],
          read_bytes: Util::Pretty.size(json['read_bytes'].to_i),
          written_rows: json['written_rows'],
          written_bytes: Util::Pretty.size(json['written_bytes'].to_i)
        }
      rescue JSON::ParserError
        {}
      end
    end
  end
end
