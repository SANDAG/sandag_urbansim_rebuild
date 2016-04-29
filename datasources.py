import urbansim.sim.simulation as sim


@sim.table('building_sqft_per_job', cache=True)
def building_sqft_per_job(store):
    return store['building_sqft_per_job']


@sim.table('household_controls', cache=True)
def household_controls(store):
    return store['household_controls']


@sim.table('scheduled_development_events', cache=True)
def scheduled_development_events(store):
    return store['scheduled_development_events']
