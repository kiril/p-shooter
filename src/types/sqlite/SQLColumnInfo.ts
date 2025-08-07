export default interface SQLColumnInfo {
  cid: number
  dflt_value: unknown
  name: string
  notnull: number
  pk: number
  type: string
}