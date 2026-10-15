-- Dedicated local Function example table in the isolated staging test project.
CREATE TABLE IF NOT EXISTS public.electric_items (
  id integer PRIMARY KEY,
  label text NOT NULL,
  enabled boolean NOT NULL DEFAULT true
);
INSERT INTO public.electric_items (id, label) VALUES (1, 'First item')
ON CONFLICT (id) DO NOTHING;
