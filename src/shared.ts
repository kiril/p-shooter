export type PSHDataType = 'V8'|'V16'|'V32'|'REAL'|'INT'|'BOOLEAN'|'BLOB'|'TEXT'

export const dataTypeToColumnType = (dt: PSHDataType) => {
  switch (dt) {
    case 'V8': return 'VARCHAR(8)'
    case 'V16': return 'VARCHAR(16)'
    case 'V32': return 'VARCHAR(32)'
    case 'REAL': return 'REAL'
    case 'INT': return 'INTEGER'
    case 'BOOLEAN': return 'INTEGER'
    case 'BLOB': return 'BLOB'
    case 'TEXT': return 'TEXT'
  }
}