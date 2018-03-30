class PgHaMigrations::Railtie < Rails::Railtie
  rake_tasks do
    load 'tasks/blocking_transactions.rake'
  end
end
