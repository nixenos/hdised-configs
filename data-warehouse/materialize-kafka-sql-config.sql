CREATE SOURCE hdised_dw_json_source
FROM KAFKA BROKER 'localhost:9092' TOPIC 'hdised' FORMAT BYTES;


CREATE MATERIALIZED VIEW jsonified_hdised_dw_source AS
SELECT data->>'source_ip' AS source_ip,
              data->>'request_timestamp' AS request_timestamp,
                     data->>'request_http_method' AS request_http_method,
                            data->>'request_endpoint' AS request_endpoint,
                                   data->>'request_http_version' AS request_http_version,
                                          data->>'response_code' AS response_code,
                                                 data->>'response_bytes_count' AS response_bytes_count,
                                                        data->>'http_client_user_agent' AS http_client_user_agent
FROM
  (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data
   FROM hdised_dw_json_source);


create view requests_sum_by_ip as
select json_source.source_ip,
       count(json_source.request_timestamp)
from jsonified_hdised_dw_source as json_source
group by json_source.source_ip;


select *
from requests_sum_by_ip;


create view request_endpoint_fetch_frequency as
select json_source.request_endpoint,
       count(json_source.request_timestamp)
from jsonified_hdised_dw_source as json_source
group by json_source.request_endpoint;
