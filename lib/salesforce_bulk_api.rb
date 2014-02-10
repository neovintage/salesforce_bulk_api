require 'rubygems'
require 'bundler'
Bundler.require()
require "salesforce_bulk_api/version"
require 'net/https'
require 'yajl/json_gem'
require 'nokogiri'
require 'csv'
require 'salesforce_bulk_api/job'
require 'salesforce_bulk_api/connection'
require 'active_support/core_ext/hash/conversions'

module SalesforceBulkApi

  class Api

    SALESFORCE_API_VERSION = '23.0'

    def initialize(client, salesforce_api_version)
      @connection = SalesforceBulkApi::Connection.new(salesforce_api_version, client)
    end

    def upsert(sobject, records, external_field, get_response = false, send_nulls = false, no_null_list = [], batch_size = 10000, timeout = 1500)
      self.do_operation('upsert', sobject, records, external_field, get_response, timeout, batch_size, send_nulls, no_null_list)
    end

    def update(sobject, records, get_response = false, send_nulls = false, no_null_list = [], batch_size = 10000, timeout = 1500)
      self.do_operation('update', sobject, records, nil, get_response, timeout, batch_size, send_nulls, no_null_list)
    end

    def create(sobject, records, get_response = false, send_nulls = false, batch_size = 10000, timeout = 1500)
      self.do_operation('insert', sobject, records, nil, get_response, timeout, batch_size, send_nulls)
    end

    def delete(sobject, records, get_response = false, batch_size = 10000, timeout = 1500)
      self.do_operation('delete', sobject, records, nil, get_response, timeout, batch_size)
    end

    def query(sobject, query, batch_size = 10000, get_response = true, timeout = 1500)
      self.do_operation('query', sobject, query, nil, get_response, timeout, batch_size)
    end

    #private

    def do_operation(operation, sobject, records, external_field, get_response, timeout, batch_size, send_nulls = false, no_null_list = [], get_batch_data = false)
      job = SalesforceBulkApi::Job.new(operation, sobject, records, external_field, @connection)

      job.create_job(batch_size, send_nulls, no_null_list)
      operation == "query" ? job.add_query() : job.add_batches()
      response = job.close_job
      hash = Hash.from_xml(response.to_s)
      hash.merge!({'batches' => Hash.from_xml(job.get_job_result(get_batch_data, timeout).to_s)}) if get_response == true

      hash
    end
  end
end
