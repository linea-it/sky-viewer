'use client';

import { AladinContext } from './AladinContext';
import { useAladin } from './useAladin';

export function AladinProvider({ children, aladinParams = {}, userGroups = [], baseHost, isDev }) {
  const aladin = useAladin(aladinParams, userGroups, baseHost, isDev);

  return (
    <AladinContext.Provider value={aladin}>
      {children}
    </AladinContext.Provider>
  );
}
