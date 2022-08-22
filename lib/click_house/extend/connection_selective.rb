# frozen_string_literal: true

module ClickHouse
  module Extend
    module ConnectionSelective
      # @return [ResultSet]
      def select_all(sql, params: {})
        response = get(body: sql, query: { default_format: 'JSON', params: params })
        Response::Factory[response]
      end

      def select_value(sql, params: {})
        response = get(body: sql, query: { default_format: 'JSON', params: params })
        Array(Response::Factory[response].first).dig(0, -1)
      end

      def select_one(sql, params: {})
        response = get(body: sql, query: { default_format: 'JSON', params: params })
        Response::Factory[response].first
      end
    end
  end
end
