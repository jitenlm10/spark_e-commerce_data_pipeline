import { Canvas, useFrame } from "@react-three/fiber";
import { OrbitControls, useGLTF, useAnimations } from "@react-three/drei";
import { useEffect, useRef } from "react";

function Model() {
  const group = useRef();

  const { scene, animations } = useGLTF("/models/shoulder2.glb");
  const { actions } = useAnimations(animations, group);

  useEffect(() => {
    const firstAction = actions ? Object.values(actions)[0] : null;
    if (firstAction) {
      firstAction.reset().fadeIn(0.3).play();
    }

    return () => {
      if (firstAction) {
        firstAction.fadeOut(0.3);
      }
    };
  }, [actions]);

  // ✅ ADD THIS ONLY
  useFrame(() => {
    if (group.current) {
      group.current.position.x = 0;
      group.current.position.y = -2.2;
      group.current.position.z = 0;
    }
  });

  return (
    <primitive
      ref={group}
      object={scene}
      scale={1.5}
      position={[0, -2.2, 0]}
    />
  );
}

export default function GuideModel() {
  useEffect(() => {
    const text = "Welcome to the portal";
    const speech = new SpeechSynthesisUtterance(text);
    speech.rate = 1;
    speech.pitch = 1;
    speech.volume = 1;

    speechSynthesis.cancel();
    speechSynthesis.speak(speech);

    return () => {
      speechSynthesis.cancel();
    };
  }, []);

  return (
    <div style={{ width: "100%", height: "420px" }}>
      <Canvas camera={{ position: [0, 2, 5], fov: 40 }}>
        <ambientLight intensity={1.5} />
        <directionalLight position={[5, 5, 5]} intensity={2.5} />
        <Model />
        <OrbitControls enableZoom={false} enablePan={false} />
      </Canvas>
    </div>
  );
}