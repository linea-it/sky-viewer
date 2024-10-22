'use client';
import React, { useState } from 'react';
import Image from 'next/image';

export default function SupportImage({ href, src, alt }) {
  const [isHovered, setIsHovered] = useState(false);

  const getImageDimensions = (alt) => {
    switch (alt) {
      case "INCT":
        return { width: 60, height: 60 };
      default:
        return { width: 100, height: 60 };
    }
  };

  const { width, height } = getImageDimensions(alt);

  return (
    <a href={href} target="_blank" rel="noreferrer">
      <Image
        src={src}
        alt={alt}
        width={width}
        height={height}
        style={{
          filter: isHovered ? 'grayscale(0%)' : 'grayscale(100%)',
          transition: 'filter 0.3s ease',
        }}
        onMouseOver={() => setIsHovered(true)}
        onMouseOut={() => setIsHovered(false)}
      />
    </a>
  );
}
