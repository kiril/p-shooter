export type PSHDatabaseQueryOperator = '=' | '>' | '<' | '>=' | '<=' | '!=' | 'in' | 'not in' | 'like'
export type PSHDatabaseQueryCondition = [PSHDatabaseQueryOperator, PSHDatabaseQueryValue] | PSHDatabaseQueryValue
export type PSHDatabaseQueryValue = string | number | boolean | null
type PSHDatabaseQuery = Record<string,PSHDatabaseQueryCondition>
export default PSHDatabaseQuery