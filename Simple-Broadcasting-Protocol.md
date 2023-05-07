# Simple Broadcasting Protocol

- Packet Format

  | Field Name | Length (in bytes) | Data Type | Description           |
  | ---------- | ----------------- | --------- | --------------------- |
  | msg header | 8                 | N/A       | contains control info |
  | msg body   | variable          | N/A       | the payload           |

  

- Header Format

  | Field Name   | Length (in bytes) | Data Type | Description                               |
  | ------------ | ----------------- | --------- | ----------------------------------------- |
  | total length | 4                 | int       | = header.length + body.length             |
  | command ID   | 4                 | int       | = 0; data request<br />= 1; data response |

  

- Data Request Msg Body

  | Field Name                 | Length (in bytes) | Data Type    | Description                               |
  | -------------------------- | ----------------- | ------------ | ----------------------------------------- |
  | timestamp of the last data | 25                | Octet String | pattern: [yyyy-MM-dd hh:mm:ss.SSS]        |
  | the last data              | 4                 | int          | a random integer in the range of [0, 100) |

  

- Data Response Msg Body

  | Field Name            | Length (in bytes) | Data Type    | Description                                                  |
  | --------------------- | ----------------- | ------------ | ------------------------------------------------------------ |
  | status code           | 1                 | Octet String | = "0"; The last history data sent by the client was not found. The server will send all its history data to the client.<br />= "1"; Indicate the data is history data.<br />= "2"; Indicate the data is real-time data |
  | timestamp of the data | 25                | Octet String | pattern: [yyyy-MM-dd hh:mm:ss.SSS]                           |
  | data                  | 4                 | int          | a random integer in the range of [0, 100)                    |

  