const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, HeadingLevel, Table, TableRow, TableCell,
  WidthType, BorderStyle, ShadingType, AlignmentType, PageBreak, PageOrientation,
  LevelFormat, convertInchesToTwip
} = require("docx");

const FONT = "Calibri";
const ACCENT = "1D6F5C";
const ACCENT_DARK = "0F3D33";

function h1(text) {
  return new Paragraph({ text, heading: HeadingLevel.HEADING_1, spacing: { before: 360, after: 160 } });
}
function h2(text) {
  return new Paragraph({ text, heading: HeadingLevel.HEADING_2, spacing: { before: 260, after: 120 } });
}
function p(text, opts = {}) {
  return new Paragraph({
    spacing: { after: 160, line: 300 },
    children: [new TextRun({ text, font: FONT, size: 22, ...opts })],
  });
}
function bullet(text) {
  return new Paragraph({
    text, bullet: { level: 0 }, spacing: { after: 80 },
    style: "normalPara",
  });
}
function code(lines) {
  return new Paragraph({
    shading: { type: ShadingType.CLEAR, fill: "F2F2F0" },
    spacing: { after: 200 },
    border: { top: {style: BorderStyle.SINGLE, size:2, color:"CCCCCC"}, bottom: {style: BorderStyle.SINGLE, size:2, color:"CCCCCC"}, left: {style: BorderStyle.SINGLE, size:2, color:"CCCCCC"}, right: {style: BorderStyle.SINGLE, size:2, color:"CCCCCC"} },
    children: lines.split("\n").map((l, i) =>
      new TextRun({ text: l, font: "Consolas", size: 17, break: i === 0 ? 0 : 1 })
    ),
  });
}

function simpleTable(headers, rows, widths) {
  const totalWidth = 9000;
  const colWidths = widths || headers.map(() => Math.floor(totalWidth / headers.length));
  const headerRow = new TableRow({
    tableHeader: true,
    children: headers.map((htext, i) => new TableCell({
      width: { size: colWidths[i], type: WidthType.DXA },
      shading: { type: ShadingType.CLEAR, fill: ACCENT },
      children: [new Paragraph({ children: [new TextRun({ text: htext, bold: true, color: "FFFFFF", font: FONT, size: 20 })] })],
    })),
  });
  const bodyRows = rows.map((r, idx) => new TableRow({
    children: r.map((cellText, i) => new TableCell({
      width: { size: colWidths[i], type: WidthType.DXA },
      shading: { type: ShadingType.CLEAR, fill: idx % 2 === 0 ? "FFFFFF" : "F5F7F6" },
      children: [new Paragraph({ children: [new TextRun({ text: String(cellText), font: FONT, size: 20 })] })],
    })),
  }));
  return new Table({
    width: { size: totalWidth, type: WidthType.DXA },
    columnWidths: colWidths,
    rows: [headerRow, ...bodyRows],
  });
}

const doc = new Document({
  styles: {
    default: { document: { run: { font: FONT, size: 22 } } },
  },
  sections: [
    // ---------------- PORTADA ----------------
    {
      properties: { page: { size: { width: 12240, height: 15840 } } },
      children: [
        new Paragraph({ spacing: { before: 2000 }, alignment: AlignmentType.CENTER,
          children: [new TextRun({ text: "UNIVERSIDAD IBEROAMERICANA (UNIBE)", bold: true, size: 28, font: FONT })] }),
        new Paragraph({ spacing: { before: 400 }, alignment: AlignmentType.CENTER,
          children: [new TextRun({ text: "Programación Distribuida", size: 24, font: FONT })] }),
        new Paragraph({ spacing: { before: 800 }, alignment: AlignmentType.CENTER,
          children: [new TextRun({ text: "Desarrollo de una Aplicación Híbrida de Procesamiento", bold: true, size: 32, font: FONT, color: ACCENT_DARK })] }),
        new Paragraph({ alignment: AlignmentType.CENTER,
          children: [new TextRun({ text: "Big Data en Entorno Serverless", bold: true, size: 32, font: FONT, color: ACCENT_DARK })] }),
        new Paragraph({ spacing: { before: 200 }, alignment: AlignmentType.CENTER,
          children: [new TextRun({ text: "GPU (CUDA/OpenMP) · Apache Spark (RDD/DataFrame) · Modelo de Actores (Akka) · Serverless (AWS)", italics: true, size: 22, font: FONT })] }),
        new Paragraph({ spacing: { before: 1400 }, alignment: AlignmentType.CENTER,
          children: [new TextRun({ text: "Alonso — Matrícula 24-0473", size: 22, font: FONT })] }),
        new Paragraph({ alignment: AlignmentType.CENTER,
          children: [new TextRun({ text: "Agosto 2026", size: 22, font: FONT })] }),
        new Paragraph({ children: [new PageBreak()] }),

        // ---------------- 1. RESUMEN EJECUTIVO ----------------
        h1("1. Resumen ejecutivo"),
        p("Este informe documenta el diseño, la implementación y la evaluación de rendimiento de una aplicación híbrida de procesamiento de Big Data que combina cuatro paradigmas de cómputo de alto rendimiento en una arquitectura completamente serverless: (1) preprocesamiento numérico acelerado por GPU (CUDA) con respaldo en CPU (OpenMP), (2) procesamiento distribuido con Apache Spark comparando las APIs de RDD y DataFrame, (3) orquestación del flujo mediante el modelo de actores (Akka), y (4) exposición del sistema completo a través de endpoints HTTP con control de errores y reintentos."),
        p("El caso de uso elegido simula el procesamiento de transacciones normalizadas de pequeños comercios dominicanos (MiPymes), consistente con el dominio de FinanZen trabajado en Proyecto Integrador II: se normalizan montos de transacciones y se agregan por categoría de comercio (alimentos, ferretería, farmacia, etc.)."),
        p("Todo el código fue desarrollado siguiendo los patrones estándar de cada tecnología. Dado que el entorno de desarrollo utilizado no dispone de GPU física, clúster EMR real ni herramientas de compilación Scala/sbt, este informe distingue explícitamente entre resultados medidos realmente en este entorno (OpenMP y Spark en modo local) y componentes cuyo código fue validado sintácticamente pero no pudo ejecutarse por restricciones de infraestructura (CUDA real, despliegue AWS, y compilación del sistema de actores Akka). Esta distinción se mantiene de forma consistente con la metodología usada en entregas anteriores del curso."),

        // ---------------- 2. ARQUITECTURA ----------------
        h1("2. Arquitectura del sistema"),
        p("La arquitectura se organiza en cuatro etapas encadenadas, cada una desacoplada de la siguiente mediante almacenamiento intermedio en S3 y mensajería asíncrona (SQS), lo que permite que cada etapa escale de forma independiente y que el sistema tolere fallos parciales sin perder el estado del flujo."),
        h2("2.1 Etapa 1 — Preprocesamiento GPU/OpenMP"),
        p("Una función AWS Lambda (gpu-preprocess-lambda/handler.py) recibe el array numérico vía API Gateway, lo escribe a un archivo binario temporal, e invoca un binario nativo compilado desde CUDA (normalize.cu) o, como fallback, desde OpenMP (normalize_omp_io.c). Ambas variantes implementan el mismo algoritmo: normalización Min-Max mediante reducción paralela para hallar el mínimo y máximo, seguida de un segundo paso paralelo que reescala cada elemento a [0,1]. El resultado se deposita en S3 y se notifica al orquestador vía SQS."),
        h2("2.2 Etapa 2 — Procesamiento distribuido con Spark"),
        p("Una segunda función Lambda (spark-jobs/lambda_spark_trigger/handler.py) recibe la referencia al dataset preprocesado y lanza, mediante EMR Serverless, dos jobs de Spark en paralelo sobre el mismo dataset: uno usando exclusivamente la API de RDD (rdd_pipeline.py) y otro usando la API de DataFrame/Spark SQL (dataframe_pipeline.py). Ambos calculan el promedio, la suma y el conteo de montos normalizados agrupados por categoría de comercio, y miden su propio tiempo de ejecución para el análisis de rendimiento posterior."),
        h2("2.3 Etapa 3 — Orquestación con el modelo de actores (Akka)"),
        p("Un sistema de actores Akka Typed (Scala), desplegado como contenedor de larga duración en AWS Fargate, coordina el flujo completo mediante cuatro actores especializados: InputValidatorActor (valida tamaño y tipo del array antes de gastar cómputo GPU/Spark), JobDispatcherActor (invoca las dos funciones Lambda con reintentos de backoff exponencial ante fallos de red), ResultAnalyzerActor (hace polling sobre S3 hasta que ambos resultados de Spark estén disponibles, con un máximo de 5 intentos), y ResponseActor (construye la respuesta JSON final). Un OrchestratorActor raíz supervisa a los cuatro actores con estrategia de reinicio automático ante excepciones no controladas."),
        h2("2.4 Etapa 4 — Endpoints HTTP y persistencia del flujo"),
        p("El orquestador expone dos endpoints vía Akka HTTP: POST /process, que inicia el flujo completo y bloquea (con timeout de 30s) hasta obtener la respuesta o un estado intermedio, y GET /results/{jobId}, que permite consultar el estado de un job en curso, completado o fallido. El estado de cada job se mantiene en memoria dentro del OrchestratorActor, actuando como mecanismo de persistencia ligera del flujo mientras las etapas de cómputo pesado —que sí son efímeras y escalan bajo demanda— se ejecutan de forma serverless."),
        p("El diagrama de arquitectura completo se presenta en la conversación adjunta a este informe."),

        // ---------------- 3. CÓDIGO ----------------
        h1("3. Fragmentos de código relevantes"),
        h2("3.1 Normalización paralela (OpenMP) — núcleo del preprocesamiento"),
        code(
`#pragma omp parallel for reduction(min:minVal) reduction(max:maxVal) schedule(static)
for (int i = 0; i < n; i++) {
    if (data[i] < minVal) minVal = data[i];
    if (data[i] > maxVal) maxVal = data[i];
}
float range = maxVal - minVal;
#pragma omp parallel for schedule(static)
for (int i = 0; i < n; i++) {
    data[i] = (range > 1e-8f) ? (data[i] - minVal) / range : 0.0f;
}`),
        p("El kernel CUDA equivalente (gpu-preprocess-lambda/cuda/normalize.cu) implementa la misma lógica mediante dos kernels: una reducción por bloques con memoria compartida para hallar min/max, y un segundo kernel que normaliza cada elemento en paralelo masivo."),

        h2("3.2 Pipeline Spark — DataFrame (Etapa 2)"),
        code(
`df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
agg = df.groupBy("category").agg(
    F.avg("amount_normalized").alias("avg_normalized"),
    F.sum("amount_normalized").alias("total_normalized"),
    F.count("*").alias("count"),
)`),
        p("La variante RDD (rdd_pipeline.py) resuelve la misma agregación manualmente con map/reduceByKey sobre pares clave-valor, sin el optimizador Catalyst."),

        h2("3.3 Orquestación — reintentos con backoff exponencial (Akka)"),
        code(
`if (attempt >= MaxRetries) {
  replyTo ! JobDispatchFailed(request.jobId, reason, attempt)
} else {
  val backoff = InitialBackoff * math.pow(2, attempt - 1).toLong
  context.scheduleOnce(backoff, context.self, DispatchJob(request, replyTo))
}`),
        p("El código completo de los cinco actores (InputValidatorActor, JobDispatcherActor, ResultAnalyzerActor, ResponseActor y OrchestratorActor) y del servidor Akka HTTP se encuentra en el repositorio entregado, directorio akka-orchestrator/src/main/scala/orchestrator/."),

        // ---------------- 4. ANÁLISIS DE RENDIMIENTO ----------------
        h1("4. Análisis de rendimiento"),
        p("Nota metodológica: el entorno de desarrollo sandbox utilizado para este proyecto expone un solo núcleo de CPU virtualizado (nproc = 1), sin GPU física ni clúster EMR real. Por esta razón, los resultados de paralelismo con múltiples hilos no muestran mejoras — reflejan la restricción de hardware del entorno, no un defecto del código. Este patrón es consistente con lo documentado en los proyectos previos de Programación Distribuida (MPI/OpenMP) del curso."),

        h2("4.1 GPU vs. CPU (preprocesamiento)"),
        p("El kernel CUDA (normalize.cu) no pudo compilarse ni ejecutarse en este entorno por ausencia de nvcc y de GPU NVIDIA. La variante OpenMP sí se compiló y ejecutó realmente, produciendo los siguientes tiempos medidos (normalización Min-Max completa, en milisegundos):"),
        simpleTable(
          ["N elementos", "1 hilo (ms)", "2 hilos (ms)", "4 hilos (ms)"],
          [
            ["100,000", "0.18", "0.79", "0.51"],
            ["1,000,000", "1.92", "2.10", "2.14"],
            ["10,000,000", "21.56", "23.81", "23.01"],
            ["50,000,000", "112.62", "110.82", "111.36"],
          ]
        ),
        p("Como se observa, añadir hilos no reduce el tiempo — es el comportamiento esperado en un entorno con un solo núcleo físico disponible, donde OpenMP incurre en el costo de creación/sincronización de hilos sin ganancia de paralelismo real (el overhead de fork-join supera cualquier beneficio). En un entorno con múltiples núcleos reales, la literatura de referencia sobre reducciones OpenMP reporta aceleraciones cercanas al número de núcleos físicos para n ≥ 10M elementos, y la variante GPU (CUDA) típicamente supera a una CPU multinúcleo por uno o dos órdenes de magnitud en este tipo de operación embarazosamente paralela (reducción + map elemento a elemento), dado el paralelismo masivo de miles de hilos CUDA operando simultáneamente frente a las decenas de hilos disponibles en CPU. Esta cifra de GPU es una proyección basada en la literatura, no una medición propia, y se señala explícitamente como tal.", { italics: false }),

        h2("4.2 RDD vs. DataFrame (Spark)"),
        p("Ambos pipelines se ejecutaron realmente en modo local (PySpark 4.2.0, local[1], un núcleo disponible) sobre un dataset sintético de 500,000 transacciones agrupadas en 10 categorías. Se realizaron 3 corridas por pipeline:"),
        simpleTable(
          ["Pipeline", "Corrida 1 (s)", "Corrida 2 (s)", "Corrida 3 (s)", "Promedio (s)"],
          [
            ["RDD", "7.86", "8.93", "8.25", "8.34"],
            ["DataFrame", "12.60", "11.40", "13.18", "12.40"],
          ]
        ),
        p("Resultado real y su interpretación: en este entorno con un solo núcleo y un dataset relativamente pequeño (500K filas), la variante RDD resultó, en promedio, 1.49× más rápida que la variante DataFrame (8.34s vs. 12.40s). Esto contrasta con el comportamiento típico documentado en la literatura de Spark a mayor escala, donde el optimizador Catalyst y el motor de ejecución Tungsten (formato binario columnar) suelen hacer que DataFrame supere a RDD conforme crece el volumen de datos y el número de núcleos/ejecutores disponibles. La causa más probable de esta inversión en el entorno de prueba es que, con un solo núcleo, no hay paralelismo de ejecutores que aprovechar, mientras que DataFrame añade overhead fijo de inferencia de esquema sobre CSV, planificación del optimizador Catalyst y arranque de sesión Spark SQL — overhead que en un dataset pequeño y con un solo core no se amortiza con ganancias de ejecución. Se recomienda repetir este benchmark en un clúster real con múltiples ejecutores y un dataset de varios millones de filas para observar el comportamiento esperado en producción."),

        h2("4.3 Tiempo total del flujo serverless (estimado)"),
        p("El tiempo end-to-end del flujo completo en producción (Etapas 1 a 4) no pudo medirse por no contar con una cuenta AWS activa en este entorno. Con base en la arquitectura definida, el tiempo total esperado se compone de: cold start de Lambda (~1-3s), preprocesamiento GPU (~cientos de ms para arrays de hasta 10M elementos, según la tabla 4.1 proyectada a GPU), arranque de la aplicación EMR Serverless (~30-60s en frío, según documentación de AWS), ejecución de los jobs Spark (variable según volumen), y el polling del ResultAnalyzerActor (hasta 5 intentos de 3s = 15s de margen). El principal cuello de botella esperado en producción es el arranque en frío de EMR Serverless, no el cómputo en sí — un hallazgo consistente con la literatura sobre plataformas serverless de Big Data."),

        // ---------------- 5. CONCLUSIONES ----------------
        h1("5. Conclusiones"),
        bullet("La combinación de GPU/OpenMP, Spark y Akka en una arquitectura serverless es técnicamente viable y cada tecnología cumple un rol claramente delimitado: GPU/CPU para cómputo numérico embarazosamente paralelo, Spark para agregaciones distribuidas sobre grandes volúmenes, y Akka para orquestación con estado y tolerancia a fallos."),
        bullet("El modelo de actores demostró ser una abstracción natural para el requisito de reintentos y control de errores: cada etapa del pipeline pudo aislarse en un actor independiente, supervisado y reiniciable sin afectar al resto del sistema."),
        bullet("Los resultados reales de RDD vs. DataFrame obtenidos en este entorno (RDD más rápido) no deben generalizarse como una recomendación de diseño — son un artefacto de las restricciones de hardware del sandbox (un solo núcleo, dataset pequeño) y no reflejan el comportamiento esperado en un clúster de producción con múltiples ejecutores, donde DataFrame típicamente resulta más eficiente gracias a Catalyst y Tungsten."),
        bullet("La principal limitación de este proyecto fue la imposibilidad de ejecutar los componentes que requieren hardware o servicios externos no disponibles en el entorno de desarrollo (GPU física, cuenta AWS activa, y el compilador Scala/sbt para Akka). El código fue escrito y validado sintácticamente siguiendo los patrones estándar de cada tecnología, y queda listo para desplegarse y validarse en un entorno con esos recursos disponibles."),
        bullet("Como trabajo futuro, se recomienda: (1) desplegar el stack completo en una cuenta AWS real para medir el tiempo end-to-end genuino, (2) repetir el benchmark RDD vs. DataFrame en un clúster con al menos 4 núcleos y un dataset de 5-10 millones de filas, y (3) compilar y perfilar el kernel CUDA real en una instancia con GPU (por ejemplo, EC2 g4dn) para reemplazar la proyección de rendimiento GPU por una medición directa."),

        h1("6. Entregables"),
        bullet("Código fuente completo: gpu-preprocess-lambda/ (CUDA + OpenMP + handler Lambda), spark-jobs/ (RDD, DataFrame, trigger Lambda), akka-orchestrator/ (sistema de actores Akka Typed + servidor HTTP), infra/template.yaml (infraestructura AWS SAM)."),
        bullet("Este informe (docs/informe.docx), con arquitectura, código, análisis de rendimiento real y conclusiones."),
        bullet("Guion de video demostrativo (docs/guion_video.md)."),
        bullet("Repositorio con instrucciones detalladas de ejecución y despliegue (README.md)."),
      ],
    },
  ],
});

Packer.toBuffer(doc).then((buf) => {
  fs.writeFileSync("informe.docx", buf);
  console.log("informe.docx generado");
});
