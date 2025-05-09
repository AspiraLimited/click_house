# frozen_string_literal: true

module ClickHouse
  class Connection
    include Extend::ConnectionHealthy
    include Extend::ConnectionDatabase
    include Extend::ConnectionTable
    include Extend::ConnectionSelective
    include Extend::ConnectionInserting
    include Extend::ConnectionAltering
    include Extend::ConnectionExplaining

    Faraday::Response.register_middleware(
      raise_error: ClickHouse::Middleware::RaiseError,
      logging: ClickHouse::Middleware::Logging,
      parse_csv: ClickHouse::Middleware::ParseCsv
    )

    attr_reader :config

    # @param [Config]
    def initialize(config)
      @config = config
    end

    def execute(query, body = nil, database: config.database, params: {})
      post(body, query: { query: query }, database: database, params: config.global_params.merge(params))
    end

    # @param path [String] Clickhouse HTTP endpoint, e.g. /ping, /replica_status
    # @param body [String] SQL to run
    # @param database [String|NilClass] database to use, nil to skip
    # @param query [Hash] other CH settings to send through params, e.g. max_rows_to_read=1
    # @example get(body: 'select number from system.numbers limit 100', query: { max_rows_to_read: 10 })
    # @return [Faraday::Response]
    def get(path = '/', body: '', query: {}, database: config.database)
      # backward compatibility since
      # https://github.com/shlima/click_house/pull/12/files#diff-9c6f3f06d3b575731eae4b6b95ddbcdcc20452c432b8f6e87a3a8e8645818107R24
      if query.is_a?(String)
        query = { query: query }
        config.logger!.warn('since v1.4.0 use connection.get(body: "SELECT 1") instead of connection.get(query: "SELECT 1")')
      end

      transport.get(path) do |conn|
        conn.params = prepare_params(query.delete(:params)).merge(query).merge(database: database).compact
        conn.params[:send_progress_in_http_headers] = 1 unless body.empty?
        conn.body = body
      end
    end

    def post(body = nil, query: {}, database: config.database, params: {})
      transport.post(compose('/', query.merge(database: database, **params)), body)
    end

    def transport
      @transport ||= Faraday.new(config.url!) do |conn|
        conn.options.timeout = config.timeout
        conn.options.open_timeout = config.open_timeout
        conn.headers = config.headers
        conn.ssl.verify = config.ssl_verify
        conn.request :authorization, :basic, config.username, config.password if config.auth?
        conn.response :raise_error
        conn.response :logging, logger: config.logger!
        conn.response :json, content_type: %r{application/json}
        conn.response :parse_csv, content_type: %r{text/csv}
        conn.adapter config.adapter
      end
    end

    def compose(path, query = {})
      # without <query.compact> "DB::Exception: Empty query" error will occur
      "#{path}?#{URI.encode_www_form({ send_progress_in_http_headers: 1 }.merge(query).compact)}"
    end

    def prepare_params(params)
      return {} if params.nil?
      params.transform_keys { |k| "param_#{k}" }
    end
  end
end
