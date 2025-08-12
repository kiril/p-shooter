import { useEffect, useState } from "react"
import Pea from "../Pea"
import PSHCollection from "../PSHCollection"


const useFresh = <Object extends Pea>(collection: PSHCollection, id?: string) => {
  const [data, setData] = useState<Object|undefined>()
  const [error, setError] = useState<Error|null>(null)

  useEffect(() => {
    setError(null)
    setData(undefined)
    if (id) {
      const unsubPromise = collection.onDoc<Object>(id, setData)
      return () => { unsubPromise.then(unsub => unsub()) }
    }
  }, [collection, id])

  return { data, error }
}

export default useFresh