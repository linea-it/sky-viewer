'use client';

import { AladinContext } from './AladinContext';
import { useAladin } from './useAladin';

export function AladinProvider({ children, aladinParams = {}, userGroups = [], baseHost }) {
  const aladin = useAladin(aladinParams, userGroups, baseHost);

  return (
    <AladinContext.Provider value={aladin}>
      {children}
    </AladinContext.Provider>
  );
}
