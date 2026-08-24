const services = ["API", "Scheduler", "Worker", "PostgreSQL", "Redis"];

export function App() {
  return (
    <main>
      <section className="hero">
        <p className="eyebrow">Task orchestration, under construction</p>
        <h1>TaskForge</h1>
        <p className="lede">
          The monorepo is online. Task scheduling and execution will arrive in a later milestone.
        </p>
      </section>
      <section aria-labelledby="services-title">
        <h2 id="services-title">Service boundaries</h2>
        <ul>
          {services.map((service) => (
            <li key={service}>{service}</li>
          ))}
        </ul>
      </section>
    </main>
  );
}

