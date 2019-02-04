class PgHaMigrations::Railtie < Rails::Railtie
  rake_tasks do
    load 'tasks/blocking_transactions.rake'
    load 'tasks/modify_migrations.rake'
  end
end
