# frozen_string_literal: true

require 'faraday'
require 'csv'

module ClickHouse
  module Middleware
    class ParseCsv < Faraday::Middleware
      def initialize(app, content_type: /text\/csv/)
        super(app)
        @content_type = content_type
      end

      def call(env)
        @app.call(env).on_complete do |e|
          parse(e) if parse_response?(e)
        end
      end

      private

      def parse_response?(env)
        content_type = env.response_headers['content-type']
        content_type&.match?(@content_type)
      end

      def parse(env)
        body = env.body
        return if body.nil? || body.strip.empty?

        env.body = CSV.parse(body)
      end
    end
  end
end

