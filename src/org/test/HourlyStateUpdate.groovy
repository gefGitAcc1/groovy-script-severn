
def ctx = binding.variables.get('applicationContext')
def hbs = ctx.getBean('hourlyBonusService')

def state = hbs.findUserBonusInfo(1l, true)

state.freeAttempts = 1
state.numberOfCollected = 3
state.lastCollectedTime = 0

hbs.saveHourlyUserBonus(1l, state)

'OK'