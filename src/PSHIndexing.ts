import PSHColumnDef from './PSHColumnDef'
import { dataTypeToColumnType, PSHDataType } from './shared'


const indexPathToField = (path: string) => path.replace('.', '__')
const fieldToIndexPath = (field: string) => field.replace('__', '.')
const toFieldName = (ix: PSHIndexSpec): string => ix.fields.map(({ path }) => indexPathToField(path)).join('___')
const toIndex = (ix: PSHIndexSpec, colName: string): string => `${toFieldName(ix)} ON ${colName} (${ix.fields.map(ip => indexPathToField(ip.path)).join(', ')})`
const toColumns = (ix: PSHIndexSpec): PSHColumnDef[] => ix.fields.map(({ path, type }) => ({ name: indexPathToField(path), type: dataTypeToColumnType(type) }))

export interface BasicIndexParams {
  path: string
  type?: PSHDataType
  unique?: boolean
}

export interface CompoundIndexParams {
  fields: PSHIndexFieldSpec[],
  unique?: boolean
}

export interface PSHIndexSpec {
  fields: PSHIndexField[]
  unique: boolean
}

export interface PSHIndexFieldSpec {
  path: string
  type?: PSHDataType
}

export interface PSHIndexField {
  path: string
  type: PSHDataType
}

const make = (params: BasicIndexParams|CompoundIndexParams|string): PSHIndexSpec => {
  if (typeof params === 'string') {
    const path = params as string
    return {
      fields: [{ path, type: 'V32' }],
      unique: false
    }
  }

  const basic = params as BasicIndexParams
  if (basic.path) {
    return {
      fields: [{ path: basic.path, type: basic.type || 'V32' }],
      unique: basic.unique || false
    }
  }

  const compound = params as CompoundIndexParams
  return {
    fields: compound.fields.map(({ path, type }) => ({ path, type: type || 'V32' })),
    unique: compound.unique || false
  }
}

export default {
  indexPathToField,
  fieldToIndexPath,
  toFieldName,
  toIndex,
  toColumns,
  make,
}