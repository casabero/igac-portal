"""Motor de renderizado cartografico para el Atlas.
Genera mapas con Matplotlib siguiendo la simbologia catastral IGAC."""

import io
import geopandas as gpd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

from .data_loader import cargar_capa, normalizar_nombre_capa, LAYER_ALIASES

# Tamanos de pagina en pulgadas
PAGE_SIZES = {
    'carta': (8.5, 11),
    'oficio': (8.5, 14),
    'plotter': (24, 36),
}

# Estilos por tipo de capa normalizado
LAYER_STYLES = {
    'terrenos_urbano': {
        'edgecolor': '#000000', 'linewidth': 0.3, 'facecolor': 'none', 'zorder': 3,
        'label_field': 'CODIGO', 'label_func': lambda c: str(c)[-4:] if c else '',
        'label_size': 5, 'label_color': '#333333',
    },
    'terrenos_rural': {
        'edgecolor': '#000000', 'linewidth': 0.3, 'facecolor': 'none', 'zorder': 3,
        'label_field': 'CODIGO', 'label_func': lambda c: str(c)[-4:] if c else '',
        'label_size': 5, 'label_color': '#333333',
    },
    'manzanas': {
        'edgecolor': '#228B22', 'linewidth': 0.8, 'facecolor': 'none', 'zorder': 2,
        'label_field': 'CODIGO', 'label_func': lambda c: str(c)[-4:] if c else '',
        'label_size': 7, 'label_color': '#228B22',
    },
    'veredas': {
        'edgecolor': '#228B22', 'linewidth': 0.8, 'facecolor': 'none', 'zorder': 2,
        'label_field': 'CODIGO', 'label_func': lambda c: str(c)[-4:] if c else '',
        'label_size': 7, 'label_color': '#228B22',
    },
    'vias_urbano': {
        'edgecolor': '#000000', 'linewidth': 0.6, 'facecolor': 'none', 'zorder': 4,
    },
    'vias_rural': {
        'edgecolor': '#000000', 'linewidth': 0.6, 'facecolor': 'none', 'zorder': 4,
    },
    'nomenclatura_vial': {
        'visible': False,
        'label_field': 'TEXTO', 'label_func': lambda c: str(c) if c else '',
        'label_size': 5, 'label_color': '#555555',
    },
    'nomenclatura_dom': {
        'edgecolor': 'none', 'linewidth': 0, 'facecolor': 'none', 'zorder': 5,
        'label_field': 'TEXTO', 'label_func': lambda c: str(c) if c else '',
        'label_size': 4, 'label_color': '#666666',
    },
    'construcciones': {
        'edgecolor': '#888888', 'linewidth': 0.2, 'facecolor': '#EEEEEE', 'zorder': 1,
    },
    'construcciones_rural': {
        'edgecolor': '#888888', 'linewidth': 0.2, 'facecolor': '#EEEEEE', 'zorder': 1,
    },
}

# Estilo para predio seleccionado
SELECTED_STYLE = {
    'edgecolor': '#FF0000', 'linewidth': 1.5, 'facecolor': '#FF000015', 'zorder': 10,
}


def _get_style_for_layer(layer_name):
    """Obtiene el estilo para una capa por su nombre."""
    norm = normalizar_nombre_capa(layer_name)
    alias = LAYER_ALIASES.get(norm, norm)
    return LAYER_STYLES.get(alias, {
        'edgecolor': '#999999', 'linewidth': 0.3, 'facecolor': 'none', 'zorder': 0
    })


def _add_labels(ax, gdf, style, bounds=None):
    """Agrega etiquetas a las geometrias segun las reglas del estilo."""
    label_field = style.get('label_field')
    label_func = style.get('label_func')
    if not label_field or not label_func:
        return

    # Buscar columna case-insensitive
    col_match = None
    for col in gdf.columns:
        if col.upper() == label_field.upper():
            col_match = col
            break

    if not col_match:
        return

    for _, row in gdf.iterrows():
        try:
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue

            centroid = geom.centroid
            if bounds:
                if not (bounds[0] <= centroid.x <= bounds[2] and bounds[1] <= centroid.y <= bounds[3]):
                    continue

            label_text = label_func(row.get(col_match, ''))
            if not label_text:
                continue

            ax.annotate(
                label_text,
                xy=(centroid.x, centroid.y),
                fontsize=style.get('label_size', 5),
                color=style.get('label_color', '#333333'),
                ha='center', va='center',
                fontfamily='sans-serif',
                fontweight='bold',
            )
        except Exception:
            continue


def render_map(municipio_id, layers_data, selected_geom=None, selected_code=None,
               bounds=None, buffer_pct=0.15, page_size='carta',
               show_labels=True, enabled_layers=None, dpi=150, for_pdf=False):
    """
    Renderiza el mapa del atlas.

    Args:
        municipio_id: ID del municipio
        layers_data: dict {layer_name: GeoDataFrame}
        selected_geom: Shapely geometry del predio seleccionado
        selected_code: Codigo del predio para etiqueta
        bounds: (minx, miny, maxx, maxy) extent a renderizar. Si None, usa extent total.
        buffer_pct: Porcentaje de buffer alrededor del extent
        page_size: 'carta', 'oficio' o 'plotter'
        show_labels: Si mostrar etiquetas
        enabled_layers: Lista de layers habilitados. None = todos.
        dpi: Resolucion
        for_pdf: Si es True, genera PDF vectorial

    Returns:
        BytesIO con la imagen PNG o PDF
    """
    fig_w, fig_h = PAGE_SIZES.get(page_size, PAGE_SIZES['carta'])

    # Margenes para composicion cartografica
    if for_pdf:
        fig = plt.figure(figsize=(fig_w, fig_h), dpi=dpi)
        # Area del mapa (con margenes para titulo y leyenda)
        ax = fig.add_axes([0.08, 0.12, 0.84, 0.78])
    else:
        fig = plt.figure(figsize=(fig_w, fig_h), dpi=dpi)
        ax = fig.add_axes([0.02, 0.02, 0.96, 0.96])

    ax.set_facecolor('#FFFFFF')
    fig.patch.set_facecolor('#FFFFFF')

    # Calcular extent si no se proporciono
    if bounds is None:
        all_bounds = []
        for gdf in layers_data.values():
            if gdf is not None and not gdf.empty:
                b = gdf.total_bounds
                all_bounds.append(b)
        if all_bounds:
            all_b = np.array(all_bounds)
            bounds = (all_b[:, 0].min(), all_b[:, 1].min(), all_b[:, 2].max(), all_b[:, 3].max())
        else:
            bounds = (0, 0, 1, 1)

    # Aplicar buffer
    dx = (bounds[2] - bounds[0]) * buffer_pct
    dy = (bounds[3] - bounds[1]) * buffer_pct
    # Minimo buffer para predios muy pequenos
    min_buf = max(dx, dy, 10)
    dx = max(dx, min_buf)
    dy = max(dy, min_buf)
    view_bounds = (bounds[0] - dx, bounds[1] - dy, bounds[2] + dx, bounds[3] + dy)

    ax.set_xlim(view_bounds[0], view_bounds[2])
    ax.set_ylim(view_bounds[1], view_bounds[3])

    # Renderizar capas en orden de zorder
    sorted_layers = sorted(layers_data.items(),
                           key=lambda x: _get_style_for_layer(x[0]).get('zorder', 0))

    for layer_name, gdf in sorted_layers:
        if gdf is None or gdf.empty:
            continue

        if enabled_layers and layer_name not in enabled_layers:
            continue

        style = _get_style_for_layer(layer_name)

        if style.get('visible') is False:
            # Solo labels, no geometria
            if show_labels:
                _add_labels(ax, gdf, style, view_bounds)
            continue

        try:
            # Clip al extent para rendimiento
            gdf_clip = gdf.cx[view_bounds[0]:view_bounds[2], view_bounds[1]:view_bounds[3]]
            if gdf_clip.empty:
                continue

            gdf_clip.plot(
                ax=ax,
                edgecolor=style.get('edgecolor', '#999999'),
                linewidth=style.get('linewidth', 0.3),
                facecolor=style.get('facecolor', 'none'),
                zorder=style.get('zorder', 0),
            )

            if show_labels:
                _add_labels(ax, gdf_clip, style, view_bounds)

        except Exception as e:
            print(f"Error renderizando {layer_name}: {e}")

    # Renderizar predio seleccionado
    if selected_geom is not None:
        sel_gdf = gpd.GeoDataFrame(geometry=[selected_geom])
        sel_gdf.plot(
            ax=ax,
            edgecolor=SELECTED_STYLE['edgecolor'],
            linewidth=SELECTED_STYLE['linewidth'],
            facecolor=SELECTED_STYLE['facecolor'],
            zorder=SELECTED_STYLE['zorder'],
        )
        # Etiqueta del predio seleccionado
        if selected_code:
            centroid = selected_geom.centroid
            ax.annotate(
                selected_code,
                xy=(centroid.x, centroid.y),
                fontsize=8, color='#FF0000',
                ha='center', va='center',
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.2', facecolor='white', edgecolor='#FF0000', alpha=0.9),
                zorder=11,
            )

    # Estilo del eje
    ax.tick_params(labelsize=6, direction='in', length=3)
    ax.set_aspect('equal')

    # Composicion cartografica para PDF
    if for_pdf:
        # Titulo
        fig.text(0.5, 0.95, f'KARTA CATASTRAL',
                 ha='center', fontsize=14, fontweight='bold', fontfamily='sans-serif')
        if selected_code:
            fig.text(0.5, 0.925, f'Predio: {selected_code}',
                     ha='center', fontsize=10, fontfamily='sans-serif', color='#333333')

        # Escala (simplificada)
        map_width_m = view_bounds[2] - view_bounds[0]
        fig.text(0.08, 0.06, f'Ancho vista: {map_width_m:,.0f} m',
                 fontsize=7, fontfamily='sans-serif', color='#666666')

        # SRC
        fig.text(0.08, 0.04, f'Sistema de Referencia: Ver metadata del municipio',
                 fontsize=6, fontfamily='sans-serif', color='#999999')

        # Pie
        fig.text(0.92, 0.04, 'Portal IGAC // Atlas Generator',
                 ha='right', fontsize=6, fontfamily='sans-serif', color='#999999')

        # Norte (flecha simple)
        ax_north = fig.add_axes([0.88, 0.82, 0.06, 0.08])
        ax_north.annotate('N', xy=(0.5, 0.9), fontsize=10, ha='center', va='top', fontweight='bold')
        ax_north.annotate('', xy=(0.5, 0.85), xytext=(0.5, 0.1),
                          arrowprops=dict(arrowstyle='->', lw=1.5, color='black'))
        ax_north.set_xlim(0, 1)
        ax_north.set_ylim(0, 1)
        ax_north.axis('off')

    # Generar output
    output = io.BytesIO()

    if for_pdf:
        with PdfPages(output) as pdf:
            pdf.savefig(fig, dpi=dpi)
    else:
        fig.savefig(output, format='png', dpi=dpi, bbox_inches='tight',
                    facecolor='white', edgecolor='none')

    plt.close(fig)
    output.seek(0)
    return output


def render_preview(municipio_id, bounds=None, selected_geom=None, selected_code=None,
                   enabled_layers=None, show_labels=True):
    """Renderiza una preview rapida para el visor web (PNG, baja resolucion)."""
    import fiona as _fiona

    from .models import obtener_municipio

    muni = obtener_municipio(municipio_id)
    if not muni or not muni.get('gpkg_path'):
        return None

    gpkg_path = muni['gpkg_path']
    if not os.path.exists(gpkg_path):
        return None

    # Cargar capas
    layers_data = {}
    try:
        all_layers = _fiona.listlayers(gpkg_path)
    except Exception:
        return None

    for ln in all_layers:
        if enabled_layers and ln not in enabled_layers:
            continue
        try:
            if bounds:
                gdf = gpd.read_file(gpkg_path, layer=ln, bbox=bounds)
            else:
                gdf = gpd.read_file(gpkg_path, layer=ln)
            if not gdf.empty:
                layers_data[ln] = gdf
        except Exception:
            continue

    if not layers_data:
        return None

    return render_map(
        municipio_id=municipio_id,
        layers_data=layers_data,
        selected_geom=selected_geom,
        selected_code=selected_code,
        bounds=bounds,
        page_size='carta',
        show_labels=show_labels,
        enabled_layers=None,  # ya filtradas
        dpi=100,
        for_pdf=False,
    )


def render_pdf(municipio_id, bounds=None, selected_geom=None, selected_code=None,
               page_size='carta', enabled_layers=None, show_labels=True):
    """Genera un PDF vectorial de alta calidad para impresion."""
    import fiona as _fiona

    from .models import obtener_municipio

    muni = obtener_municipio(municipio_id)
    if not muni or not muni.get('gpkg_path'):
        return None

    gpkg_path = muni['gpkg_path']
    if not os.path.exists(gpkg_path):
        return None

    layers_data = {}
    try:
        all_layers = _fiona.listlayers(gpkg_path)
    except Exception:
        return None

    for ln in all_layers:
        if enabled_layers and ln not in enabled_layers:
            continue
        try:
            if bounds:
                # Buffer mas amplio para PDF (capturar contexto)
                bx = (bounds[2] - bounds[0]) * 0.3
                by = (bounds[3] - bounds[1]) * 0.3
                pdf_bbox = (bounds[0] - bx, bounds[1] - by, bounds[2] + bx, bounds[3] + by)
                gdf = gpd.read_file(gpkg_path, layer=ln, bbox=pdf_bbox)
            else:
                gdf = gpd.read_file(gpkg_path, layer=ln)
            if not gdf.empty:
                layers_data[ln] = gdf
        except Exception:
            continue

    if not layers_data:
        return None

    return render_map(
        municipio_id=municipio_id,
        layers_data=layers_data,
        selected_geom=selected_geom,
        selected_code=selected_code,
        bounds=bounds,
        page_size=page_size,
        show_labels=show_labels,
        enabled_layers=None,
        dpi=300,
        for_pdf=True,
    )


# Necesario para import de os en render_preview/render_pdf
import os
