'use client';

import { AladinContext } from './AladinContext';
import { useAladin } from './useAladin';

export function AladinProvider({ children, aladinParams = {}, userGroups = [] }) {
  const aladin = useAladin(aladinParams, userGroups);

  return (
    <AladinContext.Provider value={aladin}>
      {children}
    </AladinContext.Provider>
  );
}
